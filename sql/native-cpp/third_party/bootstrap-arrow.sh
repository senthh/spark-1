#!/usr/bin/env bash
# Build Apache Arrow C++ 15.0.2 + parquet-cpp into third_party/arrow-prefix.
# Used when the host has no Arrow packages (or a version newer than GCC 8).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PREFIX="${ARROW_PREFIX:-$ROOT/third_party/arrow-prefix}"
SRC="$ROOT/third_party/arrow-src"
VER="${ARROW_VERSION:-15.0.2}"
TARBALL="apache-arrow-${VER}.tar.gz"
URL="https://archive.apache.org/dist/arrow/arrow-${VER}/${TARBALL}"

if [[ -f "$PREFIX/lib64/pkgconfig/parquet.pc" || -f "$PREFIX/lib/pkgconfig/parquet.pc" ]]; then
  echo "Arrow already installed at $PREFIX"
  exit 0
fi

mkdir -p "$ROOT/third_party"
if [[ ! -d "$SRC/cpp" ]]; then
  echo "Downloading $URL"
  curl -fsSL "$URL" -o "$ROOT/third_party/$TARBALL"
  tar -xzf "$ROOT/third_party/$TARBALL" -C "$ROOT/third_party"
  rm -rf "$SRC"
  mv "$ROOT/third_party/apache-arrow-${VER}" "$SRC"
fi

CMAKE=cmake
command -v cmake >/dev/null || CMAKE=cmake3
GENERATOR="Unix Makefiles"
if command -v ninja >/dev/null || command -v ninja-build >/dev/null; then
  GENERATOR=Ninja
fi

BUILD_DIR="$SRC/cpp-build"
mkdir -p "$BUILD_DIR"
"$CMAKE" -S "$SRC/cpp" -B "$BUILD_DIR" -G "$GENERATOR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$PREFIX" \
  -DARROW_PARQUET=ON \
  -DARROW_WITH_SNAPPY=ON \
  -DARROW_WITH_ZLIB=ON \
  -DARROW_WITH_ZSTD=ON \
  -DARROW_WITH_LZ4=ON \
  -DARROW_IPC=ON \
  -DARROW_COMPUTE=OFF \
  -DARROW_CSV=OFF \
  -DARROW_JSON=OFF \
  -DARROW_DATASET=OFF \
  -DARROW_FLIGHT=OFF \
  -DARROW_GANDIVA=OFF \
  -DARROW_ORC=OFF \
  -DARROW_PYTHON=OFF \
  -DARROW_BUILD_TESTS=OFF \
  -DARROW_BUILD_EXAMPLES=OFF \
  -DARROW_BUILD_UTILITIES=OFF \
  -DARROW_DEPENDENCY_SOURCE=BUNDLED \
  -DARROW_SIMD_LEVEL=SSE4_2 \
  -DARROW_BUILD_SHARED=ON \
  -DARROW_BUILD_STATIC=ON

"$CMAKE" --build "$BUILD_DIR" --parallel "$(nproc 2>/dev/null || echo 4)"
"$CMAKE" --install "$BUILD_DIR"
echo "Installed Arrow ${VER} to $PREFIX"
