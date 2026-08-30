# Native SQL (experimental)

Optional C++ execution path for a supported Spark SQL subtree:

`scan → Filter col cmp literal → Project col±col → hash aggregate / hash join`

Primitive types only: `int` / `long` / `double` / `boolean`.

The planner hook is `NativeSqlStrategy` → `NativeSqlExec` in `sql/core`. It is
**off by default** (`spark.sql.nativesql.enabled=false`). Unsupported plans
fall back to the JVM engine.

## Build the JNI library

Requires Apache Arrow C++ and parquet-cpp (the same stack Velox uses for
Parquet decode). `build.sh` links them via `pkg-config` and, on Linux, copies
`libarrow` / `libparquet` next to the JNI `.so` so YARN `--files` can ship
them with `$ORIGIN` rpath.

```bash
# Rocky / RHEL 8 (pin 15.x to match GCC 8)
sudo yum install -y https://apache.jfrog.io/artifactory/arrow/almalinux/8/apache-arrow-release-latest.rpm
sudo yum install -y arrow-devel-15.0.2 parquet-devel-15.0.2

# macOS
brew install apache-arrow

# or build Arrow 15 from source into third_party/arrow-prefix
sql/native-cpp/third_party/bootstrap-arrow.sh

sql/native-cpp/build.sh
```

This writes `sql/native-cpp/build/libspark_nativesql_jni.so` (Linux) or
`.dylib` (macOS), runs C++ unit tests, and builds a microbench.

## Enable it

```bash
sql/native-cpp/build.sh
export LD_LIBRARY_PATH=$PWD/sql/native-cpp/build:${LD_LIBRARY_PATH:-}
# macOS: export DYLD_LIBRARY_PATH=$PWD/sql/native-cpp/build:$DYLD_LIBRARY_PATH

spark-sql \
  --conf spark.sql.nativesql.enabled=true \
  --conf spark.sql.nativesql.lib=$PWD/sql/native-cpp/build/libspark_nativesql_jni.so
```

On macOS use `libspark_nativesql_jni.dylib` for `spark.sql.nativesql.lib`.

## Apply these changes onto a clean Spark 4.2.0 tree

```bash
cd /path/to/spark
git fetch --tags
git checkout v4.2.0
git checkout -B tuning_spark
git apply --index sql/native-cpp/tuning_spark-nativesql.patch
# or:
sql/native-cpp/apply_tuning_spark.sh /path/to/spark
```

## Column sort

Full `ORDER BY` uses an IColumn-style index permutation: sort row ids, not
packed rows. Integer keys with `n >= 256` use LSD radix sort on `(key, index)`
pairs (8-bit passes, sign bit flipped). Smaller `n` and float/bool keys use
`std::sort` on the permutation. Multi-column sorts process key 0 first, then
`updatePermutation` re-sorts only equal ranges for keys 1, 2, ... `trySort`
skips the full sort when a range is already ordered or has a single adjacent
inversion. LIMIT / TopN is not implemented here.

IR:

```
(sort (list c0 c1) CHILD)
```

Ascending only. Keys are column refs. The permutation is applied to every
column (keys and payload).

## Tests

- C++: `sql/native-cpp/build/nativesql_test` (invoked by `build.sh`)
- Spark: `NativeSqlSuite` (skips if the JNI library is not built)
- Microbench: `sql/native-cpp/build/nativesql_bench`
