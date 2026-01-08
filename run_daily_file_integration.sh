#!/bin/bash
# Usage: ./run_daily_file_integration.sh <date> <csv_file>
# Example: ./run_daily_file_integration.sh 2025-01-01 /tmp/data/dump-2025-01-01.csv

set -e

if [ $# -ne 2 ]; then
  echo "Usage: $0 <date> <csv_file>"
  echo "Example: $0 2025-01-01 /tmp/data/dump-2025-01-01.csv"
  exit 1
fi

DATE="$1"
CSV_FILE="$2"

if [ ! -f "$CSV_FILE" ]; then
  echo "Error: CSV file not found: $CSV_FILE"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_FILE="$SCRIPT_DIR/target/Spark_Proj_AF-1.0-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f "$JAR_FILE" ]; then
  echo "Error: JAR file not found. Please run 'mvn clean package' first."
  exit 1
fi

spark-submit --class fr.esilv.spark.DailyMain --master local[*] --driver-memory 8g "$JAR_FILE" "$DATE" "$CSV_FILE"
