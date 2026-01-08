package fr.esilv.spark;

import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class RecomputeJob {

    public static void recomputeDumpAtDate(SparkSession spark,
                                           String date,
                                           String outputDir,
                                           String diffRoot) {

        // 1) lecture de tous les diffs
        Dataset<Row> diffs = spark.read().parquet(diffRoot);

        System.out.println("Recompute - schema complet des diffs :");
        diffs.printSchema();

        System.out.println("Recompute - exemple de 5 lignes :");
        diffs.show(5, false);

        long totalDiffs = diffs.count();
        System.out.println("Recompute - diffs total = " + totalDiffs);

        // 2) filtre sur day <= date
        Dataset<Row> diffsFiltered = diffs
                .filter(col("day").leq(lit(date)))
                .orderBy(col("day").asc());

        long filteredCount = diffsFiltered.count();
        System.out.println("Recompute - diffsFiltered (day <= " + date + ") = " + filteredCount);

        if (filteredCount == 0) {
            System.out.println("Recompute - aucun diff trouvé jusqu'à " + date);
            return;
        }

        // 3) colonnes métier (sans day, op)
        List<String> allCols = Arrays.stream(diffsFiltered.columns())
                .filter(c -> !c.equals("day") && !c.equals("op"))
                .collect(Collectors.toList());

        // DataFrame d'état vide avec ce schéma
        Dataset<Row> emptyWithSchema = diffsFiltered.selectExpr(allCols.toArray(new String[0])).limit(0);
        Dataset<Row> state = spark.createDataFrame(emptyWithSchema.rdd(), emptyWithSchema.schema());

        // 4) jours à rejouer
        List<String> days = diffsFiltered.select("day").distinct()
                .orderBy(col("day").asc())
                .as(Encoders.STRING())
                .collectAsList();

        System.out.println("Recompute - jours à rejouer : " + days);

        // 5) appliquer les diffs jour par jour
        for (String d : days) {
            Dataset<Row> dayDiff = diffsFiltered.filter(col("day").equalTo(lit(d)));

            Dataset<Row> inserts = dayDiff.filter(col("op").equalTo("INSERT"))
                    .selectExpr(allCols.toArray(new String[0]));
            Dataset<Row> updates = dayDiff.filter(col("op").equalTo("UPDATE"))
                    .selectExpr(allCols.toArray(new String[0]));
            Dataset<Row> deletes = dayDiff.filter(col("op").equalTo("DELETE"))
                    .selectExpr(allCols.toArray(new String[0]));

            // deletes
            Dataset<Row> deleteIds = deletes.select("uid_adresse").distinct();
            state = state.join(deleteIds,
                    state.col("uid_adresse").equalTo(deleteIds.col("uid_adresse")),
                    "left_anti");

            // updates
            Dataset<Row> updateIds = updates.select("uid_adresse").distinct();
            state = state.join(updateIds,
                    state.col("uid_adresse").equalTo(updateIds.col("uid_adresse")),
                    "left_anti");
            state = state.unionByName(updates);

            // inserts
            state = state.unionByName(inserts);

            System.out.println("Recompute - après day " + d + ", rows = " + state.count());
        }

        // 6) écrire le snapshot final
        state.write()
                .mode(SaveMode.Overwrite)
                .parquet(outputDir);

        System.out.println("Recompute - snapshot as of " + date + " écrit dans " + outputDir);
    }
}
