#!/bin/bash

# Usage: ./compute_diff_between_files.sh <parquetDirA> <parquetDirB>
# Example: ./compute_diff_between_files.sh /tmp/recap_dumpA /tmp/recap_dumpB

set -e

if [ $# -ne 2 ]; then
  echo "Usage: $0 <parquetDirA> <parquetDirB>"
  echo "Example: $0 /tmp/recap_dumpA /tmp/recap_dumpB"
  exit 1
fi

DIR_A="$1"
DIR_B="$2"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_FILE="$SCRIPT_DIR/target/Spark_Proj_AF-1.0-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f "$JAR_FILE" ]; then
  echo "Error: JAR file not found. Please run 'mvn clean package' first."
  exit 1
fi

spark-submit --class fr.esilv.spark.DiffMain --master local[*] --driver-memory 8g "$JAR_FILE" "$DIR_A" "$DIR_B"
