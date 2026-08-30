#!/usr/bin/env bash
# Build libspark_nativesql_jni for the experimental Native SQL engine.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
BUILD="$ROOT/build"
mkdir -p "$BUILD"

if [[ -z "${JAVA_HOME:-}" ]]; then
  if [[ -x /usr/libexec/java_home ]]; then
    JAVA_HOME="$(/usr/libexec/java_home)"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

OS="$(uname -s)"
case "$OS" in
  Darwin)
    JNI_OS=darwin
    LIB="$BUILD/libspark_nativesql_jni.dylib"
    ;;
  Linux)
    JNI_OS=linux
    LIB="$BUILD/libspark_nativesql_jni.so"
    ;;
  *)
    echo "Unsupported OS: $OS" >&2
    exit 1
    ;;
esac

CXX="${CXX:-c++}"
"$CXX" -std=c++17 -O3 -fPIC -shared \
  -I"$ROOT/include" \
  -I"$JAVA_HOME/include" \
  -I"$JAVA_HOME/include/$JNI_OS" \
  "$ROOT/src/engine.cpp" \
  "$ROOT/src/sort.cpp" \
  "$ROOT/src/jni_bridge.cpp" \
  -o "$LIB"

echo "Built $LIB"

# Native unit test + microbench (no JNI)
"$CXX" -std=c++17 -O3 -I"$ROOT/include" \
  "$ROOT/src/engine.cpp" \
  "$ROOT/src/sort.cpp" \
  "$ROOT/tests/nativesql_test.cpp" \
  -o "$BUILD/nativesql_test"
"$BUILD/nativesql_test"

"$CXX" -std=c++17 -O3 -I"$ROOT/include" \
  "$ROOT/src/engine.cpp" \
  "$ROOT/src/sort.cpp" \
  "$ROOT/tests/nativesql_bench.cpp" \
  -o "$BUILD/nativesql_bench"
echo "C++ tests passed. Run $BUILD/nativesql_bench for a microbench."
