# Vegam native engine

Spark-Vegam is Spark's native SQL engine. Spark talks to Vegam only.

```
Spark-Gluten-Velox:  Spark -> Substrait (Gluten) -> Velox
Spark-Vegam:         Spark -> NativePlan (Vegam) -> fused native stage
```

There is no Gluten plugin and no Substrait. That conversion layer is the
tax we skip. Velox, when `VELOX_HOME` is set, is a **kernel library
inside libvegam** (TableScan, HashAggregation, expr). It is not the
engine Spark sees.

Vegam rewrites a **whole physical stage** to `NativeStageExec` and runs a
closed IR (`NativePlan`). If the stage cannot lower, Spark keeps the
original plan.

## Enable

```
spark.sql.extensions=org.apache.spark.sql.vegam.VegamExtensions
spark.sql.vegam.enabled=true
spark.sql.vegam.backend=auto
```

`auto` uses `libvegam` when it is loaded, otherwise the IR interpreter.
`native` without the library does not rewrite. `jvm` always interprets.

## What is rewritten

Fail closed. A stage becomes `NativeStageExec` when every node lowers:

- `COUNT(*)` (footer) on parquet
- `GROUP BY` + `SUM` / `COUNT` / `MIN` / `MAX` / `AVG` (one or more)
- AND of `=`, `!=`, `>`, `>=`, `<`, `<=` (numeric or string `=`)
- Broadcast / sort-merge equijoins: inner, left, semi, anti (no extra join cond)
- `row_number` / `rank` / `dense_rank` / partition `sum` windows
- Hive-style partitioned parquet (partition values injected)
- POSIX, `file:`, `hdfs:`, `viewfs:`, `s3a:` through the **Java Hadoop client**

Not rewritten: residual expressions the cutter cannot name, AQE
`QueryStageExec` children (Final stays on Spark), join residual conditions,
libhdfs / HDFS short-circuit (intentionally unused).

## Storage

Reads use Hadoop `FileSystem` / `FSDataInputStream` (`VegamPaths.hadoopConf`).
`dfs.client.read.shortcircuit` is forced **false**. There is no `libhdfs`
`dlopen`. That is the HDFS path for TPC-DS on the cluster.

## libvegam

```
cd sql/vegam/src/main/native
cmake -S . -B build
cmake --build build
# libvegam.so / libvegam.dylib on java.library.path
```

C++ owns `createTask` / `nextBatch` / `close`. With `VELOX_HOME`, a
partial-aggregation stage (scan, filters, broadcast hash joins, partial
agg) is one Velox Task: Hive TableScan over the task's Spark file ranges,
HashJoin, and a Spark-semantics partial AggregationNode whose output
matches Spark's buffer layout. Batches cross to the JVM once, as Arrow C
Data. Window / expand / complete aggregation and shuffle-side joins fall
back to the fused C++ pipeline.

Velox is pinned by `dev/build_velox_centos.sh`; `dev/bundle_libs.sh`
packs libvegam and its shared libraries for `--archives`, and
`dev/run_tpcds_yarn.sh` runs the value-compare TPC-DS gate
(`dev/run_tpcds.py`) for Vegam or Gluten with the same resources.

Without `VELOX_HOME` the in-process hash table is used (local/Mac builds).

```
export VELOX_HOME=/home/acceldata/velox-src/velox
sql/vegam/dev/build_velox_centos.sh
```

Pinned Velox SHA is recorded by that script (`git rev-parse` in the tree).

## TPC-DS product gate

A query PASSes when row counts match Spark with vegam off. It is **native**
when every time-dominating stage is `NativeStageExec`. Shuffle / final sort
may stay Spark.

```
python3 sql/vegam/dev/run_tpcds.py \
  --queries /tmp/tpcds_bench/queries \
  --db tpcds_sf10_parquet
```

The full Spark a/b set (q1-q99 plus 14a/b, 23a/b, 39a/b) is the gate, not
Q1-Q15.
