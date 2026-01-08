package fr.esilv.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.*;

/**
 * DailyJob
 * Intègre un dump CSV BAN pour une date donnée, calcule les différences
 * par rapport au dernier snapshot et met à jour les données stockées.
 *
 * Stockage utilisé (relatif au projet) :
 * - data/bal_latest : snapshot courant
 * - data/bal_diff   : diffs journaliers partitionnés par day=YYYY-MM-DD
 */
public class DailyJob {

    // Racine data relative au répertoire courant
       private static final String DATA_ROOT = System.getProperty("user.dir") + "/bal.db";

        private static final String DIFF_ROOT   = DATA_ROOT + "/bal_diff";
        private static final String LATEST_PATH = DATA_ROOT + "/bal_latest";


    public static void runDailyIntegration(SparkSession spark,
                                           String date,
                                           String csvPath) {

        // 1) Lecture du CSV BAN du jour
        Dataset<Row> dfNew = spark.read()
                .option("header", "true")
                .option("delimiter", ",") // et pas ";"
                .option("inferSchema", "true")
                .csv(csvPath)
                .cache();


        // Adaptation pour les CSV mocks qui ont `id` au lieu de `uid_adresse`
        if (java.util.Arrays.asList(dfNew.columns()).contains("id")
                && !java.util.Arrays.asList(dfNew.columns()).contains("uid_adresse")) {
                dfNew = dfNew.withColumnRenamed("id", "uid_adresse");
        }


        System.out.println("DailyJob - date=" + date + ", rows new=" + dfNew.count());
        System.out.println("DailyJob - DIFF_ROOT=" + DIFF_ROOT + ", LATEST_PATH=" + LATEST_PATH);

        // 2) Lecture du snapshot précédent (si existe)
        Dataset<Row> dfPrev;
        boolean hasPrev = true;

        try {
                dfPrev = spark.read().parquet(LATEST_PATH).cache();

                // Adaptation pour les snapshots mocks qui auraient `id` au lieu de `uid_adresse`
                if (java.util.Arrays.asList(dfPrev.columns()).contains("id")
                        && !java.util.Arrays.asList(dfPrev.columns()).contains("uid_adresse")) {
                        dfPrev = dfPrev.withColumnRenamed("id", "uid_adresse");
                }

                System.out.println("DailyJob - previous latest found at " + LATEST_PATH);
                } catch (Exception e) {
                hasPrev = false;
                dfPrev = spark.emptyDataFrame();
                System.out.println("DailyJob - no previous latest, treating all as INSERT");
        }

        // 3) Premier jour : tout INSERT
        if (!hasPrev) {
            Dataset<Row> insertsOnly = dfNew
                    .withColumn("op", lit("INSERT"))
                    .withColumn("day", lit(date));

            insertsOnly.write()
                    .mode(SaveMode.Overwrite)
                    .partitionBy("day")
                    .parquet(DIFF_ROOT);

            dfNew.write()
                    .mode(SaveMode.Overwrite)
                    .parquet(LATEST_PATH);

            System.out.println("DailyJob - first run, inserted rows=" + dfNew.count());
            return;
        }

        // 4) Harmoniser les colonnes entre ancien et nouveau
        dfPrev = alignColumns(dfPrev, dfNew);
        dfNew = alignColumns(dfNew, dfPrev);

        // 5) Full outer join sur uid_adresse (clé métier)
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
        Column pHash = functions.sha2(
                to_json(struct(Arrays.stream(dfPrev.columns())
                        .filter(c -> !c.equals("op") && !c.equals("day"))
                        .map(c -> col("p." + c))
                        .toArray(Column[]::new))),
                256
        );

        Column nHash = functions.sha2(
                to_json(struct(Arrays.stream(dfNew.columns())
                        .filter(c -> !c.equals("op") && !c.equals("day"))
                        .map(c -> col("n." + c))
                        .toArray(Column[]::new))),
                256
        );

        Column changed = both.and(pHash.notEqual(nHash));

        Dataset<Row> updates = joined.filter(changed)
                .select(col("n.*"))
                .withColumn("op", lit("UPDATE"));

        // 6) Diff du jour -> data/bal_diff/day=YYYY-MM-DD
        Dataset<Row> diffDay = inserts.unionByName(updates).unionByName(deletes)
                .withColumn("day", lit(date));

        diffDay.write()
                .mode(SaveMode.Overwrite)
                .partitionBy("day")
                .parquet(DIFF_ROOT);

        long insertCount = inserts.count();
        long updateCount = updates.count();
        long deleteCount = deletes.count();

        System.out.println("DailyJob - inserts=" + insertCount
                + ", updates=" + updateCount
                + ", deletes=" + deleteCount);

        // 7) Recalcul de bal_latest à partir du snapshot précédent
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

        state.write()
                .mode(SaveMode.Overwrite)
                .parquet(LATEST_PATH);

        System.out.println("DailyJob - latest snapshot written to " + LATEST_PATH
                + ", rows=" + state.count());
    }

    /**
     * Ajoute les colonnes manquantes de ref dans df et réordonne les colonnes
     * dans le même ordre que ref.
     */
    private static Dataset<Row> alignColumns(Dataset<Row> df, Dataset<Row> ref) {
        List<String> refCols = Arrays.asList(ref.columns());
        Dataset<Row> res = df;
        List<String> currentCols = Arrays.asList(res.columns());

        for (String colName : refCols) {
            if (!currentCols.contains(colName)) {
                res = res.withColumn(colName, lit(null));
            }
        }

        res = res.select(refCols.stream().map(functions::col).toArray(Column[]::new));
        return res;
    }
}
