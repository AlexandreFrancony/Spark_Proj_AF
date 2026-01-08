#!/bin/bash

# run_test_scenario.sh
# Integration test script - simulates 50 days of BAL data ingestion

set -e

echo "========================================="
echo "BAL SPARK PROJECT - INTEGRATION TEST"
echo "========================================="
echo "This script will:"
echo " 1. Generate 50 days of mock CSV data"
echo " 2. Process each day incrementally"
echo " 3. Generate daily reports"
echo " 4. Test time-travel reconstruction"
echo " 5. Test diff comparison tool"
echo "========================================="
echo ""

# Detect OS for date command compatibility
OS_TYPE="$(uname -s)"

# Configuration
BASE_DIR="/tmp/spark_project_test"
START_DATE="2025-01-01"
NUM_DAYS=50

echo "Test configuration:"
echo " Base directory: $BASE_DIR"
echo " Start date: $START_DATE"
echo " Number of days: $NUM_DAYS"
echo ""

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# JAR path (à adapter à ton nom de JAR réel)
JAR_FILE="$SCRIPT_DIR/target/Spark_Proj_AF-1.0-SNAPSHOT-jar-with-dependencies.jar"

if [ ! -f "$JAR_FILE" ]; then
  echo "ERROR: JAR file not found!"
  echo "Expected at: $JAR_FILE"
  echo "Please run: mvn clean package"
  exit 1
fi

# Clean previous test data
echo ">>> Cleaning previous test data..."
rm -rf "$BASE_DIR"

mkdir -p "$BASE_DIR/inputs"

echo ""
echo ">>> Step 1: Generating mock data for $NUM_DAYS days..."
echo ""

java -cp "$JAR_FILE" fr.devinci.bigdata.MockDataGenerator \
  "$BASE_DIR/inputs" \
  "$START_DATE" \
  "$NUM_DAYS"

echo ""
echo "========================================="
echo ">>> Step 2: Processing daily files..."
echo "========================================="

for n in $(seq 0 $((NUM_DAYS - 1))); do
  if [ "$OS_TYPE" = "Darwin" ]; then
    day=$(date -v+${n}d -j -f "%Y-%m-%d" "$START_DATE" +%Y-%m-%d)
  else
    day=$(date -d "$START_DATE + $n days" +%Y-%m-%d)
  fi

  echo ""
  echo ">>> Processing Day $((n + 1))/$NUM_DAYS: $day"
  echo "----------------------------------------"

  CSV_FILE="$BASE_DIR/inputs/dump-${day}.csv"

  if [ ! -f "$CSV_FILE" ]; then
    echo "ERROR: CSV file not found: $CSV_FILE"
    exit 1
  fi

  "$SCRIPT_DIR/run_daily_file_integration.sh" "$day" "$CSV_FILE"

  if [ $((n % 10)) -eq 9 ] || [ $n -eq $((NUM_DAYS - 1)) ]; then
    echo ""
    echo ">>> Generating report after day $((n + 1))..."
    "$SCRIPT_DIR/run_report.sh"
  fi
done

echo ""
echo "========================================="
echo ">>> Step 3: Testing Time-Travel Feature"
echo "========================================="
echo ""

if [ "$OS_TYPE" = "Darwin" ]; then
  DATE_A=$(date -v+23d -j -f "%Y-%m-%d" "$START_DATE" +%Y-%m-%d)
  DATE_B=$(date -v+40d -j -f "%Y-%m-%d" "$START_DATE" +%Y-%m-%d)
else
  DATE_A=$(date -d "$START_DATE + 23 days" +%Y-%m-%d)
  DATE_B=$(date -d "$START_DATE + 40 days" +%Y-%m-%d)
fi

echo ">>> Reconstructing dump for $DATE_A..."
"$SCRIPT_DIR/recompute_and_extract_dump_at_date.sh" \
  "$DATE_A" \
  "$BASE_DIR/recap_dumpA"

echo ""
echo ">>> Reconstructing dump for $DATE_B..."
"$SCRIPT_DIR/recompute_and_extract_dump_at_date.sh" \
  "$DATE_B" \
  "$BASE_DIR/recap_dumpB"

echo ""
echo "========================================="
echo ">>> Step 4: Testing Diff Comparison Tool"
echo "========================================="
echo ""

echo ">>> Computing differences between $DATE_A and $DATE_B..."
"$SCRIPT_DIR/compute_diff_between_files.sh" \
  "$BASE_DIR/recap_dumpA" \
  "$BASE_DIR/recap_dumpB"

echo ""
echo "========================================="
echo ">>> Step 5: Final Statistics"
echo "========================================="
echo ""

echo "Data directory structure:"
echo " bal_latest: $(find data/bal_latest -name '*.parquet' 2>/dev/null | wc -l) parquet files"
echo " bal_diff partitions: $(find data/bal_diff -type d -name 'day=*' 2>/dev/null | wc -l) days"
echo ""

if [ -d "data/bal_latest" ]; then
  LATEST_SIZE=$(du -sh data/bal_latest | cut -f1)
  echo " bal_latest size: $LATEST_SIZE"
fi

if [ -d "data/bal_diff" ]; then
  DIFF_SIZE=$(du -sh data/bal_diff | cut -f1)
  echo " bal_diff size: $DIFF_SIZE"
fi

echo ""
echo "Test data locations:"
echo " Mock CSV files: $BASE_DIR/inputs/"
echo " Reconstructed dump A ($DATE_A): $BASE_DIR/recap_dumpA/"
echo " Reconstructed dump B ($DATE_B): $BASE_DIR/recap_dumpB/"
echo ""
echo "========================================="
echo ">>> INTEGRATION TEST COMPLETED SUCCESSFULLY!"
echo "========================================="
echo ""
echo "To clean up test data:"
echo " rm -rf $BASE_DIR"
echo " rm -rf $SCRIPT_DIR/data"
echo ""
