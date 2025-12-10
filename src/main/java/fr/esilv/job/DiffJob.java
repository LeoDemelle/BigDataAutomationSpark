package fr.esilv.job;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.not;

/**
 * DiffJob
 *
 * Compare deux dumps parquet (snapshot1 et snapshot2) et calcule les différences :
 *  - I : lignes présentes dans snapshot2 mais pas dans snapshot1
 *  - D : lignes présentes dans snapshot1 mais pas dans snapshot2
 *  - U : lignes avec le même ID mais des colonnes métier différentes
 *
 * Commande côté SparkMain :
 *   SparkMain diff <parquetDir1> <parquetDir2>
 *
 * Exemple :
 *   spark-submit ... diff c:/temp/dumpA c:/temp/dumpB
 */
public class DiffJob {

    private static final String ID_COLUMN = "id";

    public static void run(SparkSession spark, String parquetDir1, String parquetDir2) {
        System.out.println("===============================================");
        System.out.println("[DiffJob] START");
        System.out.println("Snapshot1 path = " + parquetDir1);
        System.out.println("Snapshot2 path = " + parquetDir2);
        System.out.println("Working directory = " + System.getProperty("user.dir"));
        System.out.println("Spark version     = " + spark.version());
        System.out.println("===============================================");

        Dataset<Row> snap1;
        Dataset<Row> snap2;

        try {
            snap1 = spark.read().parquet(parquetDir1);
        } catch (Exception e) {
            System.out.println("[DiffJob] ERROR: cannot read first parquet directory: " + parquetDir1);
            e.printStackTrace(System.out);
            System.out.println("[DiffJob] END (failure)");
            System.out.println("===============================================");
            return;
        }

        try {
            snap2 = spark.read().parquet(parquetDir2);
        } catch (Exception e) {
            System.out.println("[DiffJob] ERROR: cannot read second parquet directory: " + parquetDir2);
            e.printStackTrace(System.out);
            System.out.println("[DiffJob] END (failure)");
            System.out.println("===============================================");
            return;
        }

        long count1 = snap1.count();
        long count2 = snap2.count();

        System.out.println("[DiffJob] Snapshot1 row count = " + count1);
        System.out.println("[DiffJob] Snapshot2 row count = " + count2);

        // Vérifier la présence de la colonne ID dans les deux snapshots
        List<String> cols1 = Arrays.asList(snap1.columns());
        List<String> cols2 = Arrays.asList(snap2.columns());

        if (!cols1.contains(ID_COLUMN) || !cols2.contains(ID_COLUMN)) {
            System.out.println("[DiffJob][ERROR] ID column '" + ID_COLUMN + "' must exist in both snapshots.");
            System.out.println("[DiffJob] Columns in snapshot1 = " + String.join(", ", cols1));
            System.out.println("[DiffJob] Columns in snapshot2 = " + String.join(", ", cols2));
            System.out.println("[DiffJob] END (no diff computed)");
            System.out.println("===============================================");
            return;
        }

        // Pour simplifier, on suppose que les schemas sont compatibles et on se base
        // sur la liste de colonnes du deuxième snapshot
        List<String> allColumns = Arrays.asList(snap2.columns());

        // Colonnes non-ID à comparer
        List<String> nonIdColumns = allColumns.stream()
                .filter(c -> !c.equals(ID_COLUMN))
                .toList();

        System.out.println("[DiffJob] Non-ID columns used for comparison: " + nonIdColumns);

        // === INSERTS : dans snapshot2 mais pas dans snapshot1 ===
        Dataset<Row> inserted = snap2.join(
                        snap1.select(col(ID_COLUMN).alias(ID_COLUMN)),
                        ID_COLUMN,
                        "left_anti")
                .withColumn("op", lit("I"));

        long insertedCount = inserted.count();
        System.out.println("[DiffJob] Inserted (I) row count = " + insertedCount);

        // === DELETES : dans snapshot1 mais pas dans snapshot2 ===
        Dataset<Row> deleted = snap1.join(
                        snap2.select(col(ID_COLUMN).alias(ID_COLUMN)),
                        ID_COLUMN,
                        "left_anti")
                .withColumn("op", lit("D"));

        long deletedCount = deleted.count();
        System.out.println("[DiffJob] Deleted (D) row count = " + deletedCount);

        // === UPDATES : présents dans les deux, mais colonnes non-ID différentes ===
        Dataset<Row> joined = snap1.alias("s1").join(
                snap2.alias("s2"),
                col("s1." + ID_COLUMN).equalTo(col("s2." + ID_COLUMN)),
                "inner"
        );

        Column diffCond = lit(false);
        for (String colName : nonIdColumns) {
            Column c1 = col("s1." + colName);
            Column c2 = col("s2." + colName);
            Column colDifferent = not(c1.eqNullSafe(c2));
            diffCond = diffCond.or(colDifferent);
        }

        Dataset<Row> updated = joined
                .filter(diffCond)
                .select(col("s2.*")) // on garde la version "snapshot2" de la ligne
                .withColumn("op", lit("U"));

        long updatedCount = updated.count();
        System.out.println("[DiffJob] Updated (U) row count = " + updatedCount);

        // === UNION FINALE I + U + D =========================================
        Dataset<Row> diff = inserted
                .unionByName(updated, true)
                .unionByName(deleted, true);

        long totalDiff = diff.count();
        System.out.println("[DiffJob] Total diff rows (I+U+D) = " + totalDiff);

        System.out.println("[DiffJob] Sample of diff (I/U/D):");
        diff.show(50, false);

        System.out.println("===============================================");
        System.out.println("[DiffJob] END");
        System.out.println("===============================================");
    }
}
