#!/bin/bash

# Usage: ./run_report.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_FILE="$SCRIPT_DIR/target/Spark_Proj_AF-1.0-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f "$JAR_FILE" ]; then
  echo "Error: JAR file not found. Please run 'mvn clean package' first."
  exit 1
fi

spark-submit --class fr.esilv.spark.ReportMain --master local[*] --driver-memory 8g "$JAR_FILE"
