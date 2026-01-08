package fr.esilv.spark;

import org.apache.spark.sql.SparkSession;

public class SparkMain {

    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException(
                    "First argument must be mode: daily|report|recompute|diff");
        }

        String mode = args[0];

        SparkSession spark = SparkSession.builder()
                .appName("Spark_Proj_AF")
                .master("local[*]")
                .getOrCreate();

        try {
            switch (mode) {
                case "daily":
                    // args: daily <date> <csvPath>
                    if (args.length != 3) {
                        throw new IllegalArgumentException(
                                "Usage for daily: daily <date> <csvPath>");
                    }
                    String date = args[1];
                    String csvPath = args[2];
                    DailyJob.runDailyIntegration(spark, date, csvPath);
                    break;

                case "report":
                    // args: report
                    if (args.length != 1) {
                        throw new IllegalArgumentException(
                                "Usage for report: report");
                    }
                    ReportJob.runReport(spark);
                    break;

                case "recompute":
                    // args: recompute <date> <outputDir>
                    if (args.length != 3) {
                        throw new IllegalArgumentException(
                                "Usage for recompute: recompute <date> <outputDir>");
                    }
                    String targetDate = args[1];
                    String outputDir = args[2];
                    RecomputeJob.recomputeDumpAtDate(spark, targetDate, outputDir);
                    break;

                case "diff":
                    // args: diff <dirA> <dirB>
                    if (args.length != 3) {
                        throw new IllegalArgumentException(
                                "Usage for diff: diff <parquetDir1> <parquetDir2>");
                    }
                    String dirA = args[1];
                    String dirB = args[2];
                    DiffJob.computeDiffBetweenFiles(spark, dirA, dirB);
                    break;

                default:
                    throw new IllegalArgumentException("Unknown mode: " + mode);
            }
        } finally {
            spark.stop();
        }
    }
}
