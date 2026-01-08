package fr.esilv.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class DailyJob {

    public static void runDailyIntegration(SparkSession spark,
                                           String date,
                                           String csvPath,
                                           String diffRoot,
                                           String latestPath) {

        // Lecture du CSV BAN du jour
        Dataset<Row> dfNew = spark.read()
                .option("header", "true")
                .option("delimiter", ";")
                .option("inferSchema", "true")
                .csv(csvPath)
                .cache();

        System.out.println("DailyJob - date=" + date + ", rows new=" + dfNew.count());

        // Snapshot précédent
        Dataset<Row> dfPrev;
        boolean hasPrev = true;
        try {
            dfPrev = spark.read().parquet(latestPath).cache();
            System.out.println("DailyJob - previous latest found");
        } catch (Exception e) {
            hasPrev = false;
            dfPrev = spark.emptyDataFrame();
            System.out.println("DailyJob - no previous latest, treating all as INSERT");
        }

        // Premier jour : tout INSERT
        if (!hasPrev) {
            Dataset<Row> insertsOnly = dfNew
                    .withColumn("op", lit("INSERT"))
                    .withColumn("day", lit(date));

            insertsOnly.write()
                    .mode(SaveMode.Overwrite)
                    .partitionBy("day")
                    .parquet(diffRoot);

            dfNew.write().mode(SaveMode.Overwrite).parquet(latestPath);
            return;
        }

        // Harmoniser les colonnes
        dfPrev = alignColumns(dfPrev, dfNew);
        dfNew  = alignColumns(dfNew, dfPrev);

        // Full outer sur uid_adresse (clé)
        Column joinCond = dfPrev.col("uid_adresse").equalTo(dfNew.col("uid_adresse"));
        Dataset<Row> joined = dfPrev.alias("p").join(dfNew.alias("n"), joinCond, "full_outer");

        // Lignes seulement dans le nouveau -> INSERT
        Column onlyNew = col("p.uid_adresse").isNull().and(col("n.uid_adresse").isNotNull());
        Dataset<Row> inserts = joined.filter(onlyNew)
                .select(col("n.*"))
                .withColumn("op", lit("INSERT"));

        // Lignes seulement dans l'ancien -> DELETE
        Column onlyPrev = col("p.uid_adresse").isNotNull().and(col("n.uid_adresse").isNull());
        Dataset<Row> deletes = joined.filter(onlyPrev)
                .select(col("p.*"))
                .withColumn("op", lit("DELETE"));

        // Lignes présentes dans les deux -> UPDATE possible
        Column both = col("p.uid_adresse").isNotNull().and(col("n.uid_adresse").isNotNull());

        // Hash JSON pour comparer toutes les colonnes (hors op, day)
        Column pHash = functions.sha2(to_json(struct(Arrays.stream(dfPrev.columns())
                .filter(c -> !c.equals("op") && !c.equals("day"))
                .map(c -> col("p." + c))
                .toArray(Column[]::new))), 256);

        Column nHash = functions.sha2(to_json(struct(Arrays.stream(dfNew.columns())
                .filter(c -> !c.equals("op") && !c.equals("day"))
                .map(c -> col("n." + c))
                .toArray(Column[]::new))), 256);

        Column changed = both.and(pHash.notEqual(nHash));

        Dataset<Row> updates = joined.filter(changed)
                .select(col("n.*"))
                .withColumn("op", lit("UPDATE"));

        // Diff du jour
        Dataset<Row> diffDay = inserts.unionByName(updates).unionByName(deletes)
                .withColumn("day", lit(date));

        diffDay.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("day")
                .parquet(diffRoot);

        System.out.println("DailyJob - inserts=" + inserts.count()
                + ", updates=" + updates.count()
                + ", deletes=" + deletes.count());

        // Recalcul du latest
        Dataset<Row> state = dfPrev;

        // Deletes
        Dataset<Row> deleteIds = deletes.select("uid_adresse").distinct();
        state = state.join(deleteIds,
                state.col("uid_adresse").equalTo(deleteIds.col("uid_adresse")),
                "left_anti");

        // Updates
        Dataset<Row> updateIds = updates.select("uid_adresse").distinct();
        state = state.join(updateIds,
                state.col("uid_adresse").equalTo(updateIds.col("uid_adresse")),
                "left_anti");
        state = state.unionByName(updates.drop("op", "day"));

        // Inserts
        state = state.unionByName(inserts.drop("op", "day"));

        state.write().mode(SaveMode.Overwrite).parquet(latestPath);
    }

    // Ajoute les colonnes manquantes et réordonne comme ref
    private static Dataset<Row> alignColumns(Dataset<Row> df, Dataset<Row> ref) {
        List<String> refCols = Arrays.asList(ref.columns());
        Dataset<Row> res = df;
        for (String colName : refCols) {
            if (!Arrays.asList(res.columns()).contains(colName)) {
                res = res.withColumn(colName, lit(null));
            }
        }
        res = res.select(refCols.stream().map(functions::col).toArray(Column[]::new));
        return res;
    }
}
