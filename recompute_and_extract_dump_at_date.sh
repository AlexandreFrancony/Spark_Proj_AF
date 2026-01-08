#!/bin/bash
DATE="$1"
OUT_DIR="$2"

spark-submit --class fr.esilv.spark.SparkMain target/Spark_Proj_AF-1.0-SNAPSHOT-jar-with-dependencies.jar recompute "$DATE" "$OUT_DIR" "C:/SparkFolder/bal.db/bal_diff"
