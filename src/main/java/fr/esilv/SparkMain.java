package fr.esilv;

import org.apache.spark.sql.SparkSession;
import fr.esilv.job.DailyIntegrationJob;
import fr.esilv.job.ReportJob;
import fr.esilv.job.RecomputeDumpJob;
import fr.esilv.job.DiffJob;

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
                    System.out.println("Running job: report");
                    ReportJob.run(spark);
                    break;


                case "recompute-dump":
                    if (args.length < 3) {
                        System.err.println("Usage: SparkMain recompute-dump <day> <outputDir>");
                        System.exit(1);
                    }
                    String targetDay = args[1];
                    String outputDir = args[2];
                    System.out.println("Running job: recompute-dump, day = " + targetDay + ", outputDir = " + outputDir);
                    RecomputeDumpJob.run(spark, targetDay, outputDir);
                    break;


                case "diff":
                    if (args.length < 3) {
                        System.err.println("Usage: SparkMain diff <parquetDir1> <parquetDir2>");
                        System.exit(1);
                    }
                    String parquetDir1 = args[1];
                    String parquetDir2 = args[2];
                    System.out.println("Running job: diff, parquetDir1 = " + parquetDir1 + ", parquetDir2 = " + parquetDir2);
                    DiffJob.run(spark, parquetDir1, parquetDir2);
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
