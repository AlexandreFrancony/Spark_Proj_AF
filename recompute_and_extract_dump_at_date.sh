#!/bin/bash

# Usage: ./recompute_and_extract_dump_at_date.sh <date: yyyy-MM-dd> <outputDir>
# Example: ./recompute_and_extract_dump_at_date.sh 2025-01-24 /tmp/recap_dumpA

set -e

if [ $# -ne 2 ]; then
  echo "Usage: $0 <date: yyyy-MM-dd> <outputDir>"
  echo "Example: $0 2025-01-24 /tmp/recap_dumpA"
  exit 1
fi

DATE="$1"
OUTPUT_DIR="$2"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_FILE="$SCRIPT_DIR/target/Spark_Proj_AF-1.0-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f "$JAR_FILE" ]; then
  echo "Error: JAR file not found. Please run 'mvn clean package' first."
  exit 1
fi

spark-submit --class fr.esilv.spark.RecomputeMain --master local[*] --driver-memory 8g "$JAR_FILE" "$DATE" "$OUTPUT_DIR"
