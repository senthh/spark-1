#!/usr/bin/env bash
# Compare OSS Spark 4.2.0 (nativesql off) vs the same build with nativesql on.
# TPC-DS Q1-Q15 plus a native-eligible primitive workload.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SPARK_HOME="${SPARK_HOME:-$ROOT/dist}"
DATA="${TPCDS_DATA:-/tmp/tpcds_sf10/parquet}"
QUERIES="${QUERIES_DIR:-$ROOT/sql/core/src/test/resources/tpcds}"
OUT="${OUT_DIR:-$ROOT/sql/native-cpp/bench-results}"
LIB="${NATIVE_LIB:-$ROOT/sql/native-cpp/build/libspark_nativesql_jni.dylib}"
if [[ ! -f "$LIB" ]]; then
  LIB="$ROOT/sql/native-cpp/build/libspark_nativesql_jni.so"
fi
export JAVA_HOME="${JAVA_HOME:-/opt/homebrew/Cellar/openjdk@17/17.0.18/libexec/openjdk.jdk/Contents/Home}"
export PATH="$JAVA_HOME/bin:$PATH"

[[ -x "$SPARK_HOME/bin/spark-sql" ]] || { echo "No spark-sql at $SPARK_HOME/bin/spark-sql"; exit 2; }
[[ -d "$DATA/store_sales" ]] || { echo "TPC-DS parquet not at $DATA"; exit 2; }

mkdir -p "$OUT"
QS=(q1 q2 q3 q4 q5 q6 q7 q8 q9 q10 q11 q12 q13 q14a q14b q15)

# Register parquet dirs as temp views
setup_sql="$OUT/setup.sql"
{
  echo "CREATE DATABASE IF NOT EXISTS tpcds_sf10;"
  echo "USE tpcds_sf10;"
  for t in "$DATA"/*; do
    [[ -d "$t" ]] || continue
    name="$(basename "$t")"
    echo "DROP TABLE IF EXISTS $name;"
    echo "CREATE TABLE $name USING parquet LOCATION 'file://$t';"
  done
} > "$setup_sql"

run_mode() {
  local mode="$1"   # jvm | native
  local extra=()
  extra+=(--conf spark.sql.adaptive.enabled=true)
  extra+=(--conf spark.sql.shuffle.partitions=8)
  extra+=(--master "local[*]")
  extra+=(--driver-memory 8g)
  extra+=(--name "tpcds-42-${mode}")
  if [[ "$mode" == native ]]; then
    extra+=(--conf spark.sql.nativesql.enabled=true)
    extra+=(--conf "spark.sql.nativesql.lib=$LIB")
  else
    extra+=(--conf spark.sql.nativesql.enabled=false)
  fi

  local summary="$OUT/summary_${mode}.tsv"
  printf "query\tseconds\tstatus\tnative_offload\n" > "$summary"

  "$SPARK_HOME/bin/spark-sql" "${extra[@]}" -f "$setup_sql" >"$OUT/setup_${mode}.log" 2>&1 || {
    echo "setup failed for $mode; see $OUT/setup_${mode}.log"
    return 1
  }

  for q in "${QS[@]}"; do
    local qf="$QUERIES/${q}.sql"
    [[ -f "$qf" ]] || { echo "missing $qf"; continue; }
    local log="$OUT/${mode}_${q}.log"
    echo ">> $mode $q"
    local t0 t1 sec st
    t0=$(date +%s)
    set +e
    "$SPARK_HOME/bin/spark-sql" "${extra[@]}" \
      -e "USE tpcds_sf10;" -f "$qf" >"$log" 2>&1
    local rc=$?
    set -e
    t1=$(date +%s)
    sec=$((t1 - t0))
    st=PASS; [[ $rc -eq 0 ]] || st=FAIL
    local off=no
    if grep -q NativeSqlExec "$log"; then off=yes; fi
    printf "%s\t%d\t%s\t%s\n" "$q" "$sec" "$st" "$off" | tee -a "$summary"
  done
}

echo "=== JVM (nativesql.enabled=false) ==="
run_mode jvm
echo "=== Native (nativesql.enabled=true) ==="
run_mode native

echo
echo "=== comparison ==="
python3 - <<'PY'
import csv, pathlib
out = pathlib.Path("""$OUT""")
# placeholder replaced below
PY
python3 - "$OUT" <<'PY'
import csv, sys
from pathlib import Path
out = Path(sys.argv[1])
def load(p):
    d = {}
    with open(p) as f:
        for row in csv.DictReader(f, delimiter="\t"):
            d[row["query"]] = row
    return d
j, n = load(out/"summary_jvm.tsv"), load(out/"summary_native.tsv")
print(f"{'query':8} {'jvm_s':>8} {'nat_s':>8} {'delta_s':>8} {'speedup':>8} {'offload':>8} {'jvm':>6} {'nat':>6}")
for q in sorted(set(j)|set(n), key=lambda x: (len(x), x)):
    a, b = j.get(q, {}), n.get(q, {})
    try:
        sa, sb = int(a["seconds"]), int(b["seconds"])
        delta = sa - sb
        sp = (sa/sb) if sb else float("inf")
    except Exception:
        sa=sb=delta=0; sp=0
    print(f"{q:8} {sa:8d} {sb:8d} {delta:8d} {sp:8.2f} {b.get('native_offload','?'):>8} {a.get('status','?'):>6} {b.get('status','?'):>6}")
PY
