package fr.esilv.job;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes; // <--- AJOUT

// pour l'idempotence (accès au filesystem Hadoop)
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;


/**
 * DailyIntegrationJob
 *
 * Ce job :
 *  - lit le CSV du jour (dump BAL)
 *  - lit le snapshot précédent (bal_latest), s'il existe
 *  - calcule les différences INSERT / DELETE (les UPDATE viendront plus tard)
 *  - écrit les diffs dans bal.db/bal_diff (partitionné par day)
 *  - met à jour bal_latest avec le snapshot du jour
 *
 * Signature attendue côté SparkMain :
 *   daily-integration <day> <csvFile>
 *
 * Ex :
 *   spark-submit ... daily-integration 2025-01-01 c:/data/dump-2025-01-01
 */
public class DailyIntegrationJob {

    // ⚠ À adapter au vrai schéma BAL (id colonne)
    // Pour l'instant on part sur un nom générique "id"
    private static final String ID_COLUMN = "id";

    // Chemins de base (relatifs au répertoire courant)
    private static final String BASE_DB_DIR = "bal.db";
    private static final String DIFF_TABLE_PATH = BASE_DB_DIR + "/bal_diff";
    private static final String LATEST_SNAPSHOT_PATH = "bal_latest";

// Schéma explicite du CSV courant (exemple jouet : id,name,city)
    private static final StructType CURRENT_CSV_SCHEMA = new StructType()
            .add("id", DataTypes.StringType, false)   // clé, non nullable
            .add("name", DataTypes.StringType, true)
            .add("city", DataTypes.StringType, true);


    public static void run(SparkSession spark, String day, String csvPath) {
        System.out.println("===============================================");
        System.out.println("[DailyIntegrationJob] START for day=" + day + ", csvPath=" + csvPath);
        System.out.println("Working directory = " + System.getProperty("user.dir"));
        System.out.println("Spark version     = " + spark.version());
        System.out.println("===============================================");

        // [IDEMPOTENCE] Vérifier si la partition du jour existe déjà
        if (isDayAlreadyIntegrated(spark, day)) {
            System.out.println("[DailyIntegrationJob] Day " + day + " already integrated in '"
                    + DIFF_TABLE_PATH + "'. Skipping integration.");
            System.out.println("===============================================");
            System.out.println("[DailyIntegrationJob] END (skipped) for day=" + day);
            System.out.println("===============================================");
            return;
        }


        // 1) Lecture du CSV du jour
        Dataset<Row> current = readCurrentCsv(spark, csvPath);

        // 2) Lecture du snapshot précédent (bal_latest)
        Snapshot prevSnapshot = readPreviousSnapshotIfAny(spark, current.schema());

        Dataset<Row> prev = prevSnapshot.dataset;
        boolean previousExists = prevSnapshot.exists;

        System.out.println("[DailyIntegrationJob] prevSnapshot.exists = " + previousExists);
        System.out.println("[DailyIntegrationJob] prev count = " + prev.count());
        System.out.println("[DailyIntegrationJob] current count = " + current.count());

        // 3) Calcul des diff (I / D) – version simple
        Dataset<Row> diff = computeSimpleDiff(spark, prev, current, day);

        // 4) Écriture de la diff dans bal.db/bal_diff
        writeDiff(diff);

        // 5) Mise à jour du snapshot bal_latest
        writeLatestSnapshot(current);

        System.out.println("===============================================");
        System.out.println("[DailyIntegrationJob] END for day=" + day);
        System.out.println("===============================================");
    }

    /**
     * Lecture du CSV du jour avec header + inférence de schéma.
     */
    private static Dataset<Row> readCurrentCsv(SparkSession spark, String csvPath) {
        System.out.println("[DailyIntegrationJob] Reading current CSV: " + csvPath);

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .schema(CURRENT_CSV_SCHEMA)  // <--- schéma explicite
                .csv(csvPath);

        System.out.println("[DailyIntegrationJob] current CSV schema:");
        df.printSchema();

        System.out.println("[DailyIntegrationJob] first 5 rows of current CSV:");
        df.show(5, false);

        boolean hasIdCol = Arrays.asList(df.columns()).contains(ID_COLUMN);

        if (!df.columns()[0].equalsIgnoreCase(ID_COLUMN) && !hasIdCol) {
            System.out.println("[DailyIntegrationJob][WARN] Column '" + ID_COLUMN + "' not found in CSV schema.");
            System.out.println("[DailyIntegrationJob][WARN] You must adapt ID_COLUMN to match the real BAL primary key.");
            // Dans ce cas, on ne peut pas gérer les null IDs / doublons proprement
            return df;
        }

        // [ID QUALITY] Gestion des IDs null et des doublons
        if (hasIdCol) {
            long beforeCount = df.count();

            // 1) Supprimer les lignes où l'ID est null
            Dataset<Row> withoutNullId = df.filter(col(ID_COLUMN).isNotNull());
            long afterNullFilterCount = withoutNullId.count();

            if (afterNullFilterCount < beforeCount) {
                System.out.println("[DailyIntegrationJob][WARN] Removed "
                        + (beforeCount - afterNullFilterCount)
                        + " rows with null ID in column '" + ID_COLUMN + "'.");
            }

            // 2) Détecter les IDs dupliqués
            Dataset<Row> duplicates = withoutNullId.groupBy(col(ID_COLUMN))
                    .count()
                    .filter(col("count").gt(1));

            long duplicateIdCount = duplicates.count();
            if (duplicateIdCount > 0) {
                System.out.println("[DailyIntegrationJob][WARN] Detected " + duplicateIdCount
                        + " distinct IDs with duplicates on column '" + ID_COLUMN + "'.");
                System.out.println("[DailyIntegrationJob][WARN] Keeping first occurrence for each ID and dropping others.");
            }

            // 3) Dédupliquer : garder une seule ligne par ID
            Dataset<Row> deduped = withoutNullId.dropDuplicates(ID_COLUMN);

            System.out.println("[DailyIntegrationJob] Row count after null/duplicate ID handling = "
                    + deduped.count());

            df = deduped;
        }

        return df;
    }


    /**
     * Petit record interne pour transporter le snapshot + un flag "exists".
     */
    private record Snapshot(Dataset<Row> dataset, boolean exists) {}

    /**
     * Lecture du snapshot précédent (bal_latest) s'il existe.
     * Si rien n'existe, on renvoie un DataFrame vide avec le même schéma que "currentSchema".
     */
    private static Snapshot readPreviousSnapshotIfAny(SparkSession spark, StructType currentSchema) {
        Path latestPath = Paths.get(LATEST_SNAPSHOT_PATH);

        if (!Files.exists(latestPath)) {
            System.out.println("[DailyIntegrationJob] No previous snapshot at '" + LATEST_SNAPSHOT_PATH + "'.");
            System.out.println("[DailyIntegrationJob] Treating this as FIRST DAY: all rows will be INSERTS.");
            Dataset<Row> empty = spark.createDataFrame(spark.emptyDataFrame().javaRDD(), currentSchema);
            return new Snapshot(empty, false);
        }

        System.out.println("[DailyIntegrationJob] Reading previous snapshot from: " + LATEST_SNAPSHOT_PATH);
        Dataset<Row> prev = spark.read().parquet(LATEST_SNAPSHOT_PATH);

        System.out.println("[DailyIntegrationJob] previous snapshot schema:");
        prev.printSchema();

        System.out.println("[DailyIntegrationJob] first 5 rows of previous snapshot:");
        prev.show(5, false);

        return new Snapshot(prev, true);
    }

    /**
     * Calcul des diff :
     *  - Inserted (I) = current LEFT ANTI JOIN prev sur ID_COLUMN
     *  - Deleted  (D) = prev    LEFT ANTI JOIN current sur ID_COLUMN
     *  - Updated  (U) = lignes présentes dans les deux snapshots, mais dont au moins
     *                   une colonne non-ID a changé.
     *
     * On ajoute une colonne 'op' ("I", "U" ou "D") + une colonne 'day'.
     */
    private static Dataset<Row> computeSimpleDiff(SparkSession spark,
                                                  Dataset<Row> prev,
                                                  Dataset<Row> current,
                                                  String day) {
        System.out.println("[DailyIntegrationJob] Computing diff (I/U/D).");

        // Vérifier que la colonne ID existe bien dans les deux DataFrames
        if (!Arrays.asList(prev.columns()).contains(ID_COLUMN)
                || !Arrays.asList(current.columns()).contains(ID_COLUMN)) {
            System.out.println("[DailyIntegrationJob][WARN] ID column '" + ID_COLUMN + "' is missing in one of the datasets.");
            System.out.println("[DailyIntegrationJob][WARN] Returning an EMPTY diff for now.");
            return spark.emptyDataFrame()
                    .withColumn("op", lit("I"))
                    .withColumn("day", lit(day)); // structure minimale
        }

        // === INSERTS : dans current mais pas dans prev ======================
        Dataset<Row> inserted = current.join(
                        prev.select(col(ID_COLUMN).alias(ID_COLUMN)),
                        ID_COLUMN,
                        "left_anti")
                .withColumn("op", lit("I"));

        System.out.println("[DailyIntegrationJob] Inserted count = " + inserted.count());
        System.out.println("[DailyIntegrationJob] Sample inserted rows:");
        inserted.show(5, false);

        // === DELETES : dans prev mais pas dans current =====================
        Dataset<Row> deleted = prev.join(
                        current.select(col(ID_COLUMN).alias(ID_COLUMN)),
                        ID_COLUMN,
                        "left_anti")
                .withColumn("op", lit("D"));

        System.out.println("[DailyIntegrationJob] Deleted count = " + deleted.count());
        System.out.println("[DailyIntegrationJob] Sample deleted rows:");
        deleted.show(5, false);

        // === UPDATES : présents dans les deux, mais au moins une colonne (hors ID) a changé ===

        // On travaille avec des alias pour distinguer les colonnes prev/curr
        Dataset<Row> joined = prev.alias("prev").join(
                current.alias("curr"),
                col("prev." + ID_COLUMN).equalTo(col("curr." + ID_COLUMN)),
                "inner"
        );

        // Liste des colonnes non-ID à comparer
        List<String> allColumns = Arrays.asList(current.columns());
        List<String> nonIdColumns = allColumns.stream()
                .filter(c -> !c.equals(ID_COLUMN))
                .toList();

        System.out.println("[DailyIntegrationJob] Non-ID columns to compare for updates: " + nonIdColumns);

        // Condition "au moins une colonne non-ID a changé"
        Column diffCond = lit(false);
        for (String colName : nonIdColumns) {
            // Comparaison "null-safe" : eqNullSafe(a, b) → true même si les deux sont null
            Column prevCol = col("prev." + colName);
            Column currCol = col("curr." + colName);

            Column colDifferent = not(prevCol.eqNullSafe(currCol));
            diffCond = diffCond.or(colDifferent);
        }

        Dataset<Row> updated = joined
                .filter(diffCond)
                // On ne garde que la version "courante" de la ligne (curr.*)
                .select(col("curr.*"))
                .withColumn("op", lit("U"));

        System.out.println("[DailyIntegrationJob] Updated count = " + updated.count());
        System.out.println("[DailyIntegrationJob] Sample updated rows:");
        updated.show(5, false);

        // === UNION FINALE I + U + D ========================================
        Dataset<Row> diff = inserted
                .unionByName(updated, true)
                .unionByName(deleted, true)
                .withColumn("day", lit(day));

        System.out.println("[DailyIntegrationJob] Total diff rows = " + diff.count());
        System.out.println("[DailyIntegrationJob] Sample of diff (I/U/D):");
        diff.show(20, false);

        return diff;
    }


    /**
     * Écriture de la diff dans bal.db/bal_diff,
     * partitionnée par day, 1 fichier par jour.
     */
    private static void writeDiff(Dataset<Row> diff) {
        System.out.println("[DailyIntegrationJob] Writing diff to: " + DIFF_TABLE_PATH);

        diff.repartition(1) // un fichier parquet par partition "day"
                .write()
                .mode(SaveMode.Append)
                .partitionBy("day")
                .parquet(DIFF_TABLE_PATH);

        System.out.println("[DailyIntegrationJob] Diff written successfully.");
    }

    /**
     * Mise à jour du snapshot complet bal_latest.
     * On garde seulement le snapshot du jour, en écrasant l'ancien.
     */
    private static void writeLatestSnapshot(Dataset<Row> current) {
        System.out.println("[DailyIntegrationJob] Writing latest snapshot to: " + LATEST_SNAPSHOT_PATH);

        current.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(LATEST_SNAPSHOT_PATH);

        System.out.println("[DailyIntegrationJob] Latest snapshot written successfully.");
    }

    // [IDEMPOTENCE] Vérifie si bal.db/bal_diff/day=<day> existe déjà (HDFS ou FS local)
    private static boolean isDayAlreadyIntegrated(SparkSession spark, String day) {
        try {
            FileSystem fs = FileSystem.get(spark.sparkContext().hadoopConfiguration());
            // On utilise le Path Hadoop ici (nom complet pour éviter le conflit avec java.nio.file.Path)
            org.apache.hadoop.fs.Path dayPath =
                    new org.apache.hadoop.fs.Path(DIFF_TABLE_PATH + "/day=" + day);

            boolean exists = fs.exists(dayPath);
            System.out.println("[DailyIntegrationJob] isDayAlreadyIntegrated(" + day + ") = " + exists
                    + " (path=" + dayPath + ")");
            return exists;
        } catch (IOException e) {
            throw new RuntimeException(
                    "[DailyIntegrationJob] Failed to check if day " + day + " is already integrated", e);
        }
    }

}
