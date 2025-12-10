package fr.esilv.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 * RecomputeDumpJob
 *
 * Recalcule un snapshot complet (dump parquet) "as of" une date donnée,
 * en rejouant toutes les diffs (I / U / D) depuis le début jusqu'à cette date.
 *
 * Commande côté SparkMain :
 *   SparkMain recompute-dump <day> <outputDir>
 *
 * Exemple :
 *   spark-submit ... recompute-dump 2025-01-24 c:/temp/dumpA
 */
public class RecomputeDumpJob {

    private static final String ID_COLUMN = "id";

    // Doit être cohérent avec DailyIntegrationJob
    private static final String BASE_DB_DIR = "bal.db";
    private static final String DIFF_TABLE_PATH = BASE_DB_DIR + "/bal_diff";

    public static void run(SparkSession spark, String targetDay, String outputDir) {
        System.out.println("===============================================");
        System.out.println("[RecomputeDumpJob] START for day=" + targetDay + ", outputDir=" + outputDir);
        System.out.println("Working directory = " + System.getProperty("user.dir"));
        System.out.println("Spark version     = " + spark.version());
        System.out.println("===============================================");

        // 1) Lecture de toutes les diffs jusqu'à targetDay (inclus)
        Dataset<Row> diffs;
        try {
            diffs = spark.read().parquet(DIFF_TABLE_PATH)
                    .filter(col("day").leq(lit(targetDay)));
        } catch (Exception e) {
            System.out.println("[RecomputeDumpJob] Cannot read diff table at '" + DIFF_TABLE_PATH + "'.");
            System.out.println("[RecomputeDumpJob] Make sure daily-integration has been run before recompute-dump.");
            System.out.println("===============================================");
            System.out.println("[RecomputeDumpJob] END (no data)");
            System.out.println("===============================================");
            return;
        }

        long diffCount = diffs.count();
        if (diffCount == 0) {
            System.out.println("[RecomputeDumpJob] No diff rows found for day <= " + targetDay + ".");
            System.out.println("[RecomputeDumpJob] Nothing to recompute.");
            System.out.println("===============================================");
            System.out.println("[RecomputeDumpJob] END (no data)");
            System.out.println("===============================================");
            return;
        }

        System.out.println("[RecomputeDumpJob] Total diff rows considered = " + diffCount);

        // 2) Reconstruire le snapshot en appliquant les diffs jour par jour
        Dataset<Row> snapshot = rebuildSnapshotAtDate(spark, diffs, targetDay);

        long finalCount = snapshot.count();
        System.out.println("[RecomputeDumpJob] Final snapshot row count at day " + targetDay + " = " + finalCount);

        // 3) Écriture du snapshot dans outputDir
        snapshot.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(outputDir);

        System.out.println("[RecomputeDumpJob] Snapshot written to: " + outputDir);

        try {
            java.nio.file.Path dir = java.nio.file.Paths.get(outputDir);

            // Trouver le fichier parquet généré
            java.nio.file.Path parquetFile = java.nio.file.Files.list(dir)
                    .filter(p -> p.getFileName().toString().endsWith(".parquet"))
                    .findFirst()
                    .orElse(null);

            if (parquetFile != null) {
                // Choisir le nom final ici
                String finalName = "dump-" + targetDay + ".parquet";

                java.nio.file.Path finalPath = dir.resolve(finalName);

                java.nio.file.Files.move(
                        parquetFile,
                        finalPath,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING
                );

                System.out.println("[RecomputeDumpJob] Renamed parquet to: " + finalName);
            } else {
                System.out.println("[RecomputeDumpJob] WARNING: no parquet file found to rename.");
            }

        } catch (Exception e) {
            System.out.println("[RecomputeDumpJob] ERROR while renaming parquet file: " + e.getMessage());
        }

        
        System.out.println("===============================================");
        System.out.println("[RecomputeDumpJob] END for day=" + targetDay);
        System.out.println("===============================================");

    }

    /**
     * Rejoue les diffs (I/U/D) dans l'ordre des jours pour reconstruire le snapshot cible.
     */
    private static Dataset<Row> rebuildSnapshotAtDate(SparkSession spark,
                                                      Dataset<Row> diffs,
                                                      String targetDay) {
        // On suppose que "day" est une chaîne au format YYYY-MM-DD, donc l'ordre lexicographique
        // correspond à l'ordre chronologique.
        Dataset<Row> filtered = diffs.filter(col("day").leq(lit(targetDay)));

        // Colonnes présentes dans les diffs
        List<String> allColumns = Arrays.asList(filtered.columns());

        if (!allColumns.contains(ID_COLUMN)) {
            throw new IllegalStateException("[RecomputeDumpJob] ID column '" + ID_COLUMN + "' not found in diffs.");
        }
        if (!allColumns.contains("op") || !allColumns.contains("day")) {
            throw new IllegalStateException("[RecomputeDumpJob] Columns 'op' and/or 'day' not found in diffs.");
        }

        // Snapshot initial vide avec le bon schéma (on enlève juste op et day)
        Dataset<Row> snapshot = filtered.limit(0).drop("op", "day");

        // Liste des jours à traiter, triés
        List<String> days = filtered.select("day")
                .distinct()
                .orderBy("day")
                .as(Encoders.STRING())
                .collectAsList();

        System.out.println("[RecomputeDumpJob] Days to replay = " + days);

        for (String day : days) {
            System.out.println("[RecomputeDumpJob] Applying diffs for day=" + day);

            Dataset<Row> dayDiff = filtered.filter(col("day").equalTo(lit(day)));

            // I et U : upsert
            Dataset<Row> upserts = dayDiff
                    .filter(col("op").isin("I", "U"))
                    .drop("op", "day");

            // D : delete
            Dataset<Row> deletes = dayDiff
                    .filter(col("op").equalTo(lit("D")))
                    .select(col(ID_COLUMN))
                    .distinct();

            // 1) Appliquer les deletes : on supprime ces IDs du snapshot courant
            if (!deletes.isEmpty()) {
                snapshot = snapshot.join(deletes, ID_COLUMN, "left_anti");
            }

            // 2) Appliquer les I/U : upsert = supprimer les anciennes lignes pour ces IDs, puis ajouter les nouvelles
            if (!upserts.isEmpty()) {
                Dataset<Row> idsToUpsert = upserts.select(col(ID_COLUMN)).distinct();
                Dataset<Row> snapshotWithoutUpsertIds = snapshot.join(idsToUpsert, ID_COLUMN, "left_anti");
                // On réordonne l'union par nom de colonnes pour éviter les surprises
                snapshot = snapshotWithoutUpsertIds.unionByName(upserts, true);
            }

            System.out.println("[RecomputeDumpJob] Snapshot row count after day " + day + " = " + snapshot.count());
        }

        return snapshot;
    }
}
