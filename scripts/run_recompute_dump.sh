#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $(basename "$0") <day> <outputDir>"
  exit 1
fi

DAY="$1"
OUTPUT_DIR="$2"

# À adapter :
APP_JAR="${APP_JAR:-target/Project-1.0-SNAPSHOT.jar}"
MAIN_CLASS="${MAIN_CLASS:-fr.esilv.SparkMain}"
MASTER_URL="${MASTER_URL:-local[*]}"

spark-submit \
  --class "$MAIN_CLASS" \
  --master "$MASTER_URL" \
  "$APP_JAR" \
  recompute-dump "$DAY" "$OUTPUT_DIR"
