# Vegam native engine

Vegam rewrites a **whole physical stage** to `NativeStageExec` and runs a
closed IR (`NativePlan`). No Substrait. If the stage cannot lower, Spark
keeps the original plan.

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

C++ owns `createTask` / `nextPage` / `close`. With `VELOX_HOME`, HashAgg
is Velox TableScan (parquet) plus Velox HashAggregation. Byte ranges come
from `HadoopBytes.pread` (Java Hadoop FS). No libhdfs. Vectors stay in
Velox until the stage edge (`nextPage`).

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
