#!/bin/bash
P1="$1"
P2="$2"

spark-submit --class fr.esilv.spark.SparkMain target/Spark_Proj_AF-1.0-SNAPSHOT-jar-with-dependencies.jar diff "$P1" "$P2"
