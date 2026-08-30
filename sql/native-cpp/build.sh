#!/usr/bin/env bash
# Build libspark_nativesql_jni for the experimental Native SQL engine.
# Requires Apache Arrow C++ + parquet-cpp (pkg-config: parquet arrow).
# Pin Arrow 15 on RHEL/Rocky 8:  yum install -y arrow-devel-15.0.2 parquet-devel-15.0.2
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

# Prefer an in-tree Arrow prefix (bootstrap), then the system pkg-config.
PREFIX="${ARROW_PREFIX:-$ROOT/third_party/arrow-prefix}"
if [[ -f "$PREFIX/lib64/pkgconfig/parquet.pc" ]]; then
  export PKG_CONFIG_PATH="$PREFIX/lib64/pkgconfig:${PKG_CONFIG_PATH:-}"
elif [[ -f "$PREFIX/lib/pkgconfig/parquet.pc" ]]; then
  export PKG_CONFIG_PATH="$PREFIX/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
fi

CXX="${CXX:-c++}"
PARQUET_CFLAGS="$(pkg-config --cflags parquet arrow 2>/dev/null || true)"
PARQUET_LIBS="$(pkg-config --libs parquet arrow 2>/dev/null || true)"
if [[ -z "$PARQUET_LIBS" ]]; then
  echo "parquet-cpp / Arrow not found (pkg-config parquet)." >&2
  echo "Install Arrow 15 + parquet-cpp, or set ARROW_PREFIX." >&2
  echo "  Rocky/RHEL 8: yum install -y arrow-devel-15.0.2 parquet-devel-15.0.2" >&2
  echo "  macOS:        brew install apache-arrow" >&2
  echo "  from source:  $ROOT/third_party/bootstrap-arrow.sh" >&2
  exit 1
fi
PARQUET_LIBDIR="$(pkg-config --variable=libdir parquet 2>/dev/null || true)"

# $ORIGIN first so YARN --files can drop this .so next to libarrow/libparquet.
RPATH_FLAGS=()
if [[ "$OS" == Darwin ]]; then
  RPATH_FLAGS+=("-Wl,-rpath,@loader_path")
  if [[ -n "$PARQUET_LIBDIR" ]]; then
    RPATH_FLAGS+=("-Wl,-rpath,$PARQUET_LIBDIR")
  fi
else
  RPATH_FLAGS+=("-Wl,-rpath,\$ORIGIN")
  if [[ -n "$PARQUET_LIBDIR" ]]; then
    RPATH_FLAGS+=("-Wl,-rpath,$PARQUET_LIBDIR")
  fi
  PARQUET_LIBS="$PARQUET_LIBS -ldl"
fi

"$CXX" -std=c++17 -O3 -fPIC -shared \
  -I"$ROOT/include" \
  -I"$JAVA_HOME/include" \
  -I"$JAVA_HOME/include/$JNI_OS" \
  $PARQUET_CFLAGS \
  "$ROOT/src/engine.cpp" \
  "$ROOT/src/sort.cpp" \
  "$ROOT/src/parquet_scan.cpp" \
  "$ROOT/src/jni_bridge.cpp" \
  $PARQUET_LIBS \
  "${RPATH_FLAGS[@]}" \
  -o "$LIB"

echo "Built $LIB"

# Copy Arrow/parquet (and their non-system deps) beside the JNI lib for YARN --files.
bundle_runtime_libs() {
  local dest="$BUILD"
  if [[ "$OS" != Linux ]]; then
    return 0
  fi
  if ! command -v ldd >/dev/null; then
    return 0
  fi
  local so
  # ldd prints "lib => /abs/path (addr)" or "lib => not found"
  while read -r so; do
    [[ -z "$so" || ! -f "$so" ]] && continue
    case "$so" in
      /lib/*|/lib64/*|/usr/lib/*|/usr/lib64/*)
        # Keep Arrow/parquet even if they landed in /usr/lib64.
        local base
        base="$(basename "$so")"
        case "$base" in
          libarrow*|libparquet*|libarrow_bundled*|libthrift*|libre2*) ;;
          *) continue ;;
        esac
        ;;
    esac
    local destf="$dest/$(basename "$so")"
    if [[ "$(readlink -f "$so")" != "$(readlink -f "$destf")" ]]; then
      cp -a "$so" "$dest/"
    fi
    # Also copy the SONAME symlink target if this is a linker name.
    if [[ -L "$so" ]]; then
      local real
      real="$(readlink -f "$so")"
      if [[ -n "$real" && -f "$real" && "$(readlink -f "$dest/$(basename "$real")")" != "$real" ]]; then
        cp -a "$real" "$dest/"
      fi
    fi
  done < <(ldd "$LIB" | awk '/=>/ {print $3}')
  ls -1 "$dest"/libarrow* "$dest"/libparquet* 2>/dev/null | awk -F/ '{print $NF}' \
    > "$dest/nativesql_native_libs.txt" || true
  echo "Bundled Arrow/parquet runtime libs in $dest"
  cat "$dest/nativesql_native_libs.txt" 2>/dev/null || true
}
bundle_runtime_libs

# Native unit test + microbench (no JNI)
"$CXX" -std=c++17 -O3 -I"$ROOT/include" \
  $PARQUET_CFLAGS \
  "$ROOT/src/engine.cpp" \
  "$ROOT/src/sort.cpp" \
  "$ROOT/src/parquet_scan.cpp" \
  "$ROOT/tests/nativesql_test.cpp" \
  $PARQUET_LIBS \
  "${RPATH_FLAGS[@]}" \
  -o "$BUILD/nativesql_test"
"$BUILD/nativesql_test"

"$CXX" -std=c++17 -O3 -I"$ROOT/include" \
  $PARQUET_CFLAGS \
  "$ROOT/src/engine.cpp" \
  "$ROOT/src/sort.cpp" \
  "$ROOT/src/parquet_scan.cpp" \
  "$ROOT/tests/nativesql_bench.cpp" \
  $PARQUET_LIBS \
  "${RPATH_FLAGS[@]}" \
  -o "$BUILD/nativesql_bench"
echo "C++ tests passed. Run $BUILD/nativesql_bench for a microbench."
