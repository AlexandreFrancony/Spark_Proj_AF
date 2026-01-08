package fr.esilv.spark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import static org.apache.spark.sql.functions.*;

public class RecomputeJob {

    private static final String DIFF_ROOT   = "C:/SparkFolder/data/bal_diff";
    private static final String LATEST_PATH = "C:/SparkFolder/data/bal_latest";

    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * Recalcule un dump tel qu’il était à une date donnée,
     * en partant du dernier snapshot et en “remontant le temps”
     * grâce aux diffs.
     */
    public static void recomputeDumpAtDate(SparkSession spark,
                                           String targetDateStr,
                                           String outputDir) {

        System.out.println("RecomputeJob - targetDate=" + targetDateStr
                + ", outputDir=" + outputDir);
        System.out.println("RecomputeJob - DIFF_ROOT=" + DIFF_ROOT
                + ", LATEST_PATH=" + LATEST_PATH);

        // Validation date
        LocalDate targetDate = LocalDate.parse(targetDateStr, DATE_FMT);

        // 1) Charger le snapshot courant
        Dataset<Row> current;
        try {
            current = spark.read().parquet(LATEST_PATH).cache();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Aucun snapshot courant trouvé dans " + LATEST_PATH
                            + " (exécuter l’intégration quotidienne d’abord)", e);
        }

        long currentCount = current.count();
        System.out.println("RecomputeJob - current latest rows=" + currentCount);

        // 2) Charger tous les diffs
        Dataset<Row> diffs;
        try {
            diffs = spark.read().parquet(DIFF_ROOT).cache();
        } catch (Exception e) {
            System.out.println("RecomputeJob - aucun diff trouvé, on garde le snapshot courant.");
            saveSnapshot(current, outputDir);
            return;
        }

        // 3) Ne garder que les diffs STRICTEMENT après la date cible
        Dataset<Row> diffsAfter = diffs.filter(col("day").gt(lit(targetDateStr)));
        long countAfter = diffsAfter.count();
        System.out.println("RecomputeJob - diffs after targetDate=" + countAfter);

        if (countAfter == 0) {
            // Aucun diff après la date cible -> l’état courant = état à cette date
            System.out.println("RecomputeJob - aucun diff après la date cible, " +
                    "on utilise le snapshot courant comme résultat.");
            saveSnapshot(current, outputDir);
            return;
        }

        // 4) Appliquer les diffs à l’envers pour remonter le temps
        Dataset<Row> reconstructed = applyReverseDiffs(current, diffsAfter);

        long finalCount = reconstructed.count();
        System.out.println("RecomputeJob - reconstructed rows=" + finalCount);

        // 5) Sauvegarde
        saveSnapshot(reconstructed, outputDir);
    }

    /**
     * Applique les diffs dans le sens inverse du temps :
     * - INSERT  -> suppression des lignes insérées après la date cible
     * - DELETE  -> réinsertion des lignes supprimées
     * - UPDATE  -> restauration de l’ancienne version (stockée dans le diff)
     */
    private static Dataset<Row> applyReverseDiffs(Dataset<Row> latest,
                                                  Dataset<Row> diffsAfter) {

        // On suppose que chaque ligne de diff contient l’état "après" l’opération,
        // avec colonnes métier + op + day et clé uid_adresse.

        // 1) Lignes ajoutées après la date cible -> à supprimer
        Dataset<Row> inserts = diffsAfter.filter(col("op").equalTo("INSERT"));
        Dataset<Row> insertIds = inserts.select("uid_adresse").distinct();

        Dataset<Row> state = latest.join(
                insertIds,
                latest.col("uid_adresse").equalTo(insertIds.col("uid_adresse")),
                "left_anti");

        // 2) Lignes supprimées après la date cible -> à réinsérer
        Dataset<Row> deletes = diffsAfter.filter(col("op").equalTo("DELETE"))
                .drop("op", "day");
        Dataset<Row> deleteIds = deletes.select("uid_adresse").distinct();

        // On enlève d’abord les éventuels doublons présents du fait des updates précédentes
        state = state.join(
                deleteIds,
                state.col("uid_adresse").equalTo(deleteIds.col("uid_adresse")),
                "left_anti");

        state = state.unionByName(deletes);

        // 3) Lignes modifiées après la date cible -> à restaurer
        Dataset<Row> updates = diffsAfter.filter(col("op").equalTo("UPDATE"))
                .drop("op", "day");
        Dataset<Row> updateIds = updates.select("uid_adresse").distinct();

        state = state.join(
                updateIds,
                state.col("uid_adresse").equalTo(updateIds.col("uid_adresse")),
                "left_anti");

        state = state.unionByName(updates);

        return state;
    }

    private static void saveSnapshot(Dataset<Row> df, String outputDir) {
        System.out.println("RecomputeJob - saving snapshot to " + outputDir);
        df.write()
                .mode(SaveMode.Overwrite)
                .parquet(outputDir);
        System.out.println("RecomputeJob - save completed.");
    }
}
