package fr.esilv.spark;

import org.apache.spark.sql.SparkSession;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;

public class DiffMain {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: DiffMain <parquetDirA> <parquetDirB>");
            System.err.println("Example: DiffMain C:/SparkFolder/dumpA C:/SparkFolder/dumpB");
            System.exit(1);
        }

        String dirA = args[0];
        String dirB = args[1];

        // Log fichier
        try {
            File logDir = new File("logs");
            if (!logDir.exists()) {
                logDir.mkdirs();
            }
            FileOutputStream fos = new FileOutputStream("logs/diff.log", false);
            PrintStream fileOut = new PrintStream(fos, true, "UTF-8");

            System.setOut(new PrintStream(new TeeOutputStream(System.out, fileOut), true, "UTF-8"));
            System.setErr(new PrintStream(new TeeOutputStream(System.err, fileOut), true, "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
        }

        SparkSession spark = SparkSession.builder()
                .appName("BAL Diff")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", "true")
                .getOrCreate();

        try {
            System.out.println("==== DiffMain ====");
            System.out.println("dirA = " + dirA);
            System.out.println("dirB = " + dirB);
            System.out.println("==================");

            DiffJob.computeDiffBetweenFiles(spark, dirA, dirB);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            spark.stop();
        }
    }

    public static class TeeOutputStream extends java.io.OutputStream {
        private final java.io.OutputStream out1;
        private final java.io.OutputStream out2;

        public TeeOutputStream(java.io.OutputStream out1, java.io.OutputStream out2) {
            this.out1 = out1;
            this.out2 = out2;
        }

        @Override
        public void write(int b) throws java.io.IOException {
            out1.write(b);
            out2.write(b);
        }

        @Override
        public void flush() throws java.io.IOException {
            out1.flush();
            out2.flush();
        }

        @Override
        public void close() throws java.io.IOException {
            out1.close();
            out2.close();
        }
    }
}
