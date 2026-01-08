package fr.esilv.spark;

import org.apache.spark.sql.SparkSession;

public class SparkMain {

    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("First argument must be mode: daily|report|recompute|diff");
        }

        String mode = args[0];

        SparkSession spark = SparkSession.builder()
                .appName("SparkProjAF")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        switch (mode) {
            case "daily":
                // args: daily <date> <csvFile> <diffRoot> <latestPath>
                DailyJob.runDailyIntegration(spark, args[1], args[2], args[3], args[4]);
                break;
            case "report":
                // args: report <latestPath>
                ReportJob.runReport(spark, args[1]);
                break;
            case "recompute":
                // args: recompute <date> <outputDir> <diffRoot>
                RecomputeJob.recomputeDumpAtDate(spark, args[1], args[2], args[3]);
                break;
            case "diff":
                // args: diff <parquetDir1> <parquetDir2>
                DiffJob.computeDiffBetweenFiles(spark, args[1], args[2]);
                break;
            default:
                throw new IllegalArgumentException("Unknown mode: " + mode);
        }

        spark.stop();
    }
}
