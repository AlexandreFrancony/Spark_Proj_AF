package fr.esilv.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class ReportJob {

    public static void runReport(SparkSession spark, String latestPath) {
        Dataset<Row> df = spark.read().parquet(latestPath);

        System.out.println("==== Report: nombre d'adresses par commune_insee ====");

        Dataset<Row> agg = df.groupBy("commune_insee")
                .count()
                .orderBy("commune_insee");

        agg.show(200, false);

        System.out.println("==== Report: nombre d'adresses par commune_nom ====");

        Dataset<Row> agg2 = df.groupBy("commune_nom")
                .count()
                .orderBy(desc("count"));

        agg2.show(50, false);
    }
}
