package fr.esilv.job;

import org.apache.spark.sql.SparkSession;

public class DailyIntegrationJob {

    public static void run(SparkSession spark, String day, String csvPath) {
        System.out.println("Running DailyIntegrationJob for day = " + day + ", file = " + csvPath);

        // TODO:
        // 1. Lire le CSV du jour
        // 2. Lire bal_latest (si existe)
        // 3. Calculer les diffs (I/U/D)
        // 4. Écrire dans bal.db/bal_diff partitionné par day
        // 5. Mettre à jour bal_latest
    }
}
