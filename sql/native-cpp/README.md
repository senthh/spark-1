# Native SQL (experimental)

Optional C++ execution path for a supported Spark SQL subtree:

`scan → Filter col cmp literal → Project col±col → hash aggregate / hash join`

Primitive types only: `int` / `long` / `double` / `boolean`.

The planner hook is `NativeSqlStrategy` → `NativeSqlExec` in `sql/core`. It is
**off by default** (`spark.sql.nativesql.enabled=false`). Unsupported plans
fall back to the JVM engine.

## Build the JNI library

```bash
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

## Tests

- C++: `sql/native-cpp/build/nativesql_test` (invoked by `build.sh`)
- Spark: `NativeSqlSuite` (skips if the JNI library is not built)
- Microbench: `sql/native-cpp/build/nativesql_bench`
