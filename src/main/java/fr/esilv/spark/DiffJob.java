package fr.esilv.spark;

import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.*;

public class DiffJob {

    public static void computeDiffBetweenFiles(SparkSession spark,
                                               String path1,
                                               String path2) {

        // 1) Lecture des deux snapshots
        Dataset<Row> df1 = spark.read().parquet(path1);
        Dataset<Row> df2 = spark.read().parquet(path2);

        System.out.println("DiffJob - initial snapshot1 rows = " + df1.count());
        System.out.println("DiffJob - initial snapshot2 rows = " + df2.count());

        // 2) Échantillon : 1 000 000 lignes max, triées par uid_adresse
        df1 = df1.orderBy(col("uid_adresse")).limit(1_000_000);
        df2 = df2.orderBy(col("uid_adresse")).limit(1_000_000);

        System.out.println("DiffJob - after limit, snapshot1 rows = " + df1.count());
        System.out.println("DiffJob - after limit, snapshot2 rows = " + df2.count());

        // 3) Harmoniser les colonnes
        df1 = alignColumns(df1, df2);
        df2 = alignColumns(df2, df1);

        // 4) Full outer join sur uid_adresse
        Column joinCond = df1.col("uid_adresse").equalTo(df2.col("uid_adresse"));
        Dataset<Row> joined = df1.alias("a").join(df2.alias("b"), joinCond, "full_outer");

        // INSERT: présent seulement dans snapshot2
        Column onlyB = col("a.uid_adresse").isNull().and(col("b.uid_adresse").isNotNull());
        Dataset<Row> inserts = joined.filter(onlyB)
                .select(col("b.*"))
                .withColumn("op", lit("INSERT"));

        // DELETE: présent seulement dans snapshot1
        Column onlyA = col("a.uid_adresse").isNotNull().and(col("b.uid_adresse").isNull());
        Dataset<Row> deletes = joined.filter(onlyA)
                .select(col("a.*"))
                .withColumn("op", lit("DELETE"));

        // UPDATE: même uid_adresse mais contenu différent
        Column both = col("a.uid_adresse").isNotNull().and(col("b.uid_adresse").isNotNull());

        List<String> cols = Arrays.stream(df1.columns())
                .filter(c -> !c.equals("op"))
                .collect(Collectors.toList());

        Column aHash = sha2(to_json(struct(
                cols.stream().map(c -> col("a." + c)).toArray(Column[]::new)
        )), 256);
        Column bHash = sha2(to_json(struct(
                cols.stream().map(c -> col("b." + c)).toArray(Column[]::new)
        )), 256);

        Column changed = both.and(aHash.notEqual(bHash));

        Dataset<Row> updates = joined.filter(changed)
                .select(col("b.*"))
                .withColumn("op", lit("UPDATE"));

        long insertCount = inserts.count();
        long updateCount = updates.count();
        long deleteCount = deletes.count();

        System.out.println("DiffJob - inserts=" + insertCount
                + ", updates=" + updateCount
                + ", deletes=" + deleteCount);
    }

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
