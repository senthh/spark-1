#!/usr/bin/env bash
# Land the Native SQL changes on a Spark clone at v4.2.0 / branch tuning_spark.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
PATCH="$HERE/tuning_spark-nativesql.patch"
SPARK_DIR="${1:-}"

if [[ -z "$SPARK_DIR" ]]; then
  echo "Usage: $0 /path/to/spark" >&2
  exit 1
fi
if [[ ! -f "$PATCH" ]]; then
  echo "Missing $PATCH" >&2
  exit 1
fi

cd "$SPARK_DIR"
git fetch --tags
git checkout v4.2.0
git checkout -B tuning_spark
git apply --index "$PATCH"
echo "Applied Native SQL patch on branch tuning_spark (v4.2.0)."
echo "Next: sql/native-cpp/build.sh"
