#!/bin/bash
DATE="$1"
CSV_FILE="$2"

spark-submit --class fr.esilv.spark.SparkMain target/Spark_Proj_AF-1.0-SNAPSHOT-jar-with-dependencies.jar daily "$DATE" "$CSV_FILE" "C:/SparkFolder/bal.db/bal_diff" "C:/SparkFolder/bal_latest"
