package fr.esilv;

import org.apache.spark.sql.SparkSession;
import fr.esilv.job.DailyIntegrationJob;

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

        spark.sparkContext().setLogLevel("ERROR");

        try {
            switch (job) {
                case "daily-integration":
                    if (args.length < 3) {
                        System.err.println("Usage: SparkMain daily-integration <day> <csvFile>");
                        System.exit(1);
                    }
                    String day = args[1];
                    String csvFile = args[2];
                    System.out.println("Running job: daily-integration, day = " + day + ", csvFile = " + csvFile);
                    DailyIntegrationJob.run(spark, day, csvFile);
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
