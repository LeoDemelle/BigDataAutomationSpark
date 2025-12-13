#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $(basename "$0") <parquetDir1> <parquetDir2>"
  exit 1
fi

PARQUET_DIR1="$1"
PARQUET_DIR2="$2"

# À adapter :
APP_JAR="${APP_JAR:-target/Project-1.0-SNAPSHOT.jar}"
MAIN_CLASS="${MAIN_CLASS:-fr.esilv.SparkMain}"
MASTER_URL="${MASTER_URL:-local[*]}"

spark-submit \
  --driver-memory 3G \
  --executor-memory 6G \
  --class "$MAIN_CLASS" \
  --master "$MASTER_URL" \
  "$APP_JAR" \
  diff "$PARQUET_DIR1" "$PARQUET_DIR2"
