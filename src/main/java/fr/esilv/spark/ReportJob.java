package fr.esilv.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class ReportJob {

    // Même répertoire que DailyJob
    // Racine data relative au répertoire courant
    private static final String DATA_ROOT = System.getProperty("user.dir") + "/bal.db";
    private static final String DIFF_ROOT   = DATA_ROOT + "/bal_diff";
    private static final String LATEST_PATH = DATA_ROOT + "/bal_latest";



    public static void runReport(SparkSession spark) {

        System.out.println("ReportJob - LATEST_PATH=" + LATEST_PATH);

        Dataset<Row> df;
        try {
            df = spark.read().parquet(LATEST_PATH).cache();
        } catch (Exception e) {
            System.out.println();
            System.out.println("========================================");
            System.out.println("BAL REPORT");
            System.out.println("========================================");
            System.out.println("Aucun snapshot trouvé dans " + LATEST_PATH);
            System.out.println("Veuillez d'abord exécuter l'intégration quotidienne.");
            System.out.println("========================================");
            System.out.println();
            return;
        }

        long total = df.count();

        System.out.println();
        System.out.println("========================================");
        System.out.println("BAL REPORT - Latest Snapshot");
        System.out.println("========================================");
        System.out.println("Total addresses: " + String.format("%,d", total));
        System.out.println("----------------------------------------");

        // Top départements (si colonne présente)
        if (hasColumn(df, "code_departement")) {
            System.out.println();
            System.out.println("Top 20 Departments by Address Count:");
            System.out.println("----------------------------------------");
            df.groupBy("code_departement")
                    .count()
                    .orderBy(desc("count"))
                    .limit(20)
                    .show(20, false);
        }

        // Top communes (si colonne présente)
        if (hasColumn(df, "nom_commune")) {
            System.out.println();
            System.out.println("Top 10 Communes by Address Count:");
            System.out.println("----------------------------------------");
            df.groupBy("nom_commune")
                    .count()
                    .orderBy(desc("count"))
                    .limit(10)
                    .show(10, false);
        }

        // Top voies (si colonne présente)
        if (hasColumn(df, "nom_voie")) {
            System.out.println();
            System.out.println("Top 10 Most Common Street Names:");
            System.out.println("----------------------------------------");
            df.groupBy("nom_voie")
                    .count()
                    .orderBy(desc("count"))
                    .limit(10)
                    .show(10, false);
        }

        System.out.println();
        System.out.println("Dataset Schema:");
        System.out.println("----------------------------------------");
        df.printSchema();

        System.out.println();
        System.out.println("Sample Records (first 5):");
        System.out.println("----------------------------------------");
        df.show(5, false);

        System.out.println("========================================");
        System.out.println();
    }

    private static boolean hasColumn(Dataset<Row> df, String name) {
        for (String c : df.columns()) {
            if (c.equals(name)) return true;
        }
        return false;
    }
}
