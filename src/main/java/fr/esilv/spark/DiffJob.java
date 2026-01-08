package fr.esilv.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Column;

import static org.apache.spark.sql.functions.*;

public class DiffJob {

    /**
     * Compare deux dossiers Parquet (dump A et dump B) et affiche les stats :
     * - REMOVED : présent seulement dans A
     * - ADDED   : présent seulement dans B
     * - MODIFIED: même uid_adresse mais contenu différent
     * - UNCHANGED: même uid_adresse et même contenu
     */
    public static void computeDiffBetweenFiles(SparkSession spark,
                                               String dirA,
                                               String dirB) {

        System.out.println("DiffJob - dirA=" + dirA);
        System.out.println("DiffJob - dirB=" + dirB);

        Dataset<Row> dfA = spark.read().parquet(dirA).cache();
        Dataset<Row> dfB = spark.read().parquet(dirB).cache();

        long countA = dfA.count();
        long countB = dfB.count();

        System.out.println();
        System.out.println("========================================");
        System.out.println("DIFF ANALYSIS");
        System.out.println("========================================");
        System.out.println("Dataset A: " + dirA + " (" + String.format("%,d", countA) + " rows)");
        System.out.println("Dataset B: " + dirB + " (" + String.format("%,d", countB) + " rows)");
        System.out.println("----------------------------------------");

        // Vérifier la présence de la clé
        if (!hasColumn(dfA, "uid_adresse") || !hasColumn(dfB, "uid_adresse")) {
            throw new IllegalArgumentException(
                    "Les deux datasets doivent contenir une colonne 'uid_adresse' comme clé.");
        }

        // Colonnes communes pour comparaison de contenu
        String[] colsA = dfA.columns();
        String[] colsB = dfB.columns();
        String[] common = commonColumns(colsA, colsB);

        // Hash de contenu (hors uid_adresse)
        Column hashA = sha2(to_json(struct(contentColumns(common))), 256);
        Column hashB = sha2(to_json(struct(contentColumns(common))), 256);

        dfA = dfA.withColumn("content_hash", hashA);
        dfB = dfB.withColumn("content_hash", hashB);

        // REMOVED: dans A mais pas dans B
        long removed = dfA.join(dfB,
                        dfA.col("uid_adresse").equalTo(dfB.col("uid_adresse")),
                        "left_anti")
                .count();

        // ADDED: dans B mais pas dans A
        long added = dfB.join(dfA,
                        dfB.col("uid_adresse").equalTo(dfA.col("uid_adresse")),
                        "left_anti")
                .count();

        // MODIFIED: même uid_adresse mais hash différent
        long modified = dfA.as("a")
                .join(dfB.as("b"),
                        dfA.col("uid_adresse").equalTo(dfB.col("uid_adresse")),
                        "inner")
                .where(col("a.content_hash").notEqual(col("b.content_hash")))
                .count();

        // UNCHANGED: même uid_adresse et même hash
        long unchanged = dfA.as("a")
                .join(dfB.as("b"),
                        dfA.col("uid_adresse").equalTo(dfB.col("uid_adresse")),
                        "inner")
                .where(col("a.content_hash").equalTo(col("b.content_hash")))
                .count();

        System.out.println(" REMOVED   : " + String.format("%,d", removed));
        System.out.println(" ADDED     : " + String.format("%,d", added));
        System.out.println(" MODIFIED  : " + String.format("%,d", modified));
        System.out.println(" UNCHANGED : " + String.format("%,d", unchanged));
        System.out.println("----------------------------------------");
        long totalChanges = removed + added + modified;
        System.out.println(" TOTAL CHANGES: " + String.format("%,d", totalChanges));
        if (countA > 0) {
            double pct = (totalChanges * 100.0) / countA;
            System.out.println(" Change rate (vs A): " + String.format("%.2f%%", pct));
        }
        System.out.println("========================================");
        System.out.println();

        // Échantillons
        if (removed > 0) {
            System.out.println("Sample REMOVED (max 5):");
            dfA.join(dfB,
                            dfA.col("uid_adresse").equalTo(dfB.col("uid_adresse")),
                            "left_anti")
                    .drop("content_hash")
                    .show(5, false);
        }

        if (added > 0) {
            System.out.println("Sample ADDED (max 5):");
            dfB.join(dfA,
                            dfB.col("uid_adresse").equalTo(dfA.col("uid_adresse")),
                            "left_anti")
                    .drop("content_hash")
                    .show(5, false);
        }

        if (modified > 0) {
            System.out.println("Sample MODIFIED (max 5) - version B:");
            dfB.as("b")
                    .join(dfA.as("a"),
                            dfB.col("uid_adresse").equalTo(dfA.col("uid_adresse")),
                            "inner")
                    .where(col("b.content_hash").notEqual(col("a.content_hash")))
                    .select(col("b.*"))
                    .drop("content_hash")
                    .show(5, false);
        }
    }

    private static boolean hasColumn(Dataset<Row> df, String name) {
        for (String c : df.columns()) {
            if (c.equals(name)) return true;
        }
        return false;
    }

    private static String[] commonColumns(String[] a, String[] b) {
        java.util.Set<String> set = new java.util.HashSet<>();
        for (String s : a) set.add(s);
        java.util.List<String> res = new java.util.ArrayList<>();
        for (String s : b) {
            if (set.contains(s)) res.add(s);
        }
        return res.toArray(new String[0]);
    }

    // colonnes utilisées pour le hash (toutes sauf uid_adresse et content_hash)
    private static Column[] contentColumns(String[] all) {
        java.util.List<Column> cols = new java.util.ArrayList<>();
        for (String c : all) {
            if (!c.equals("uid_adresse") && !c.equals("content_hash")) {
                cols.add(col(c));
            }
        }
        return cols.toArray(new Column[0]);
    }
}
