package fr.esilv.job;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.desc;

/**
 * ReportJob
 *
 * Lit le snapshot courant (bal_latest) et affiche quelques stats agrégées.
 * Dans le dataset jouet, on affiche le nombre de lignes par "city".
 *
 * Commande côté SparkMain :
 *   SparkMain report
 */
public class ReportJob {

    private static final String LATEST_SNAPSHOT_PATH = "bal_latest";
    private static final String CITY_COLUMN = "city";

    public static void run(SparkSession spark) {
        System.out.println("===============================================");
        System.out.println("[ReportJob] START");
        System.out.println("Working directory = " + System.getProperty("user.dir"));
        System.out.println("Spark version     = " + spark.version());
        System.out.println("===============================================");

        // 1) Vérifier que le snapshot existe
        Path latestPath = Paths.get(LATEST_SNAPSHOT_PATH);
        if (!Files.exists(latestPath)) {
            System.out.println("[ReportJob] No latest snapshot found at '" + LATEST_SNAPSHOT_PATH + "'.");
            System.out.println("[ReportJob] Run 'daily-integration' at least once before 'report'.");
            System.out.println("===============================================");
            System.out.println("[ReportJob] END (no data)");
            System.out.println("===============================================");
            return;
        }

        // 2) Lecture du snapshot courant
        Dataset<Row> df = spark.read().parquet(LATEST_SNAPSHOT_PATH);

        long totalCount = df.count();
        System.out.println("[ReportJob] Latest snapshot row count = " + totalCount);

        // 3) Vérifier que la colonne "city" existe
        boolean hasCityCol = Arrays.asList(df.columns()).contains(CITY_COLUMN);
        if (!hasCityCol) {
            System.out.println("[ReportJob][WARN] Column '" + CITY_COLUMN + "' not found in latest snapshot.");
            System.out.println("[ReportJob][WARN] Available columns are: " + String.join(", ", df.columns()));
            System.out.println("===============================================");
            System.out.println("[ReportJob] END (no city column)");
            System.out.println("===============================================");
            return;
        }

        // 4) Agrégation : nombre d'adresses par city
        Dataset<Row> report = df.groupBy(col(CITY_COLUMN))
                .agg(count("*").alias("count"))
                .orderBy(desc("count"));

        System.out.println("[ReportJob] Address count by city:");
        report.show(50, false); // limite à 50 lignes pour ne pas exploser la console

        System.out.println("===============================================");
        System.out.println("[ReportJob] END");
        System.out.println("===============================================");
    }
}
