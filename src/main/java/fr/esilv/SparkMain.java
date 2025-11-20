package fr.esilv;

import org.apache.spark.sql.SparkSession;

public class SparkMain {

    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: SparkMain <job> [job-args...]");
            System.err.println("Available jobs: daily-integration, report, recompute-dump, diff");
            System.exit(1);
        }

        String job = args[0];

        // Création de la SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("BAL Project - " + job)
                // En dev local, pratique pour lancer depuis ton PC :
                .master("local[*]")
                .getOrCreate();

        try {
            switch (job) {
                case "daily-integration":
                    // TODO: appeler la classe qui gérera l'intégration quotidienne
                    System.out.println("Running job: daily-integration");
                    // ex: DailyIntegrationJob.run(spark, args);
                    break;

                case "report":
                    // TODO: job de reporting
                    System.out.println("Running job: report");
                    break;

                case "recompute-dump":
                    // TODO: job de recomposition d'un dump à une date donnée
                    System.out.println("Running job: recompute-dump");
                    break;

                case "diff":
                    // TODO: job de diff entre deux dumps parquet
                    System.out.println("Running job: diff");
                    break;

                default:
                    System.err.println("Unknown job: " + job);
                    System.exit(1);
            }
        } finally {
            spark.stop();
        }
    }
}
