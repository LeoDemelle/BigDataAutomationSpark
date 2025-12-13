#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $(basename "$0") <day> <csvFile>"
  exit 1
fi

DAY="$1"
CSV_FILE="$2"

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
  daily-integration "$DAY" "$CSV_FILE"
