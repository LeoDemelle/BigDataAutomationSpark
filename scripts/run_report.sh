#!/usr/bin/env bash
set -euo pipefail

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
  report
