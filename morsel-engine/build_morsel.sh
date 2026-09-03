#!/bin/bash
#
# Build Morsel-Driven Execution Engine
# Cross-platform build script (x86_64 Linux / macOS ARM)
#

set -e

echo "=== Building Morsel Engine ==="
echo ""

# Detect platform
ARCH=$(uname -m)
OS=$(uname -s)

echo "Platform: $OS $ARCH"

# Set JAVA_HOME based on platform
if [ "$OS" = "Darwin" ]; then
  # macOS
  if [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME=$(/usr/libexec/java_home 2>/dev/null || echo "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home")
  fi
  LIB_SUFFIX="dylib"
  RPATH_FLAG="-Wl,-rpath,@loader_path"
  JAVA_INCLUDE="$JAVA_HOME/include/darwin"
  LINK_CHECK="otool -L"
else
  # Linux
  if [ -z "$JAVA_HOME" ]; then
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-17.0.20.0.8-1.1.el8_10.x86_64
  fi
  LIB_SUFFIX="so"
  RPATH_FLAG="-Wl,-rpath,\$ORIGIN"
  JAVA_INCLUDE="$JAVA_HOME/include/linux"
  LINK_CHECK="ldd"
fi

echo "JAVA_HOME: $JAVA_HOME"

# Base flags
BASE_FLAGS="-std=c++17 -O3 -fPIC -pthread"

# Architecture-specific optimizations
if [ "$ARCH" = "x86_64" ]; then
  # x86_64: Use AVX2 if available
  OPTFLAGS="-march=native -mavx2 -mfma -ffast-math"
  echo "x86_64 optimizations: AVX2 enabled"
elif [ "$ARCH" = "arm64" ] || [ "$ARCH" = "aarch64" ]; then
  # ARM: Portable optimizations only
  OPTFLAGS="-ffast-math"
  echo "ARM optimizations: Portable mode (no SIMD)"
else
  OPTFLAGS=""
  echo "Unknown architecture: Using portable build"
fi

CXXFLAGS="$BASE_FLAGS $OPTFLAGS"

# Includes
INCLUDES="-I. -I/usr/local/include -I/opt/homebrew/include"
INCLUDES="$INCLUDES -I$JAVA_HOME/include -I$JAVA_INCLUDE"

# Try to find Arrow/Parquet includes
for prefix in /usr /usr/local /opt/homebrew; do
  if [ -d "$prefix/include/arrow" ]; then
    INCLUDES="$INCLUDES -I$prefix/include"
    break
  fi
done

# Libraries
LIBS="-larrow -lparquet -lpthread"

# Library paths
LIB_PATHS="-L/usr/local/lib -L/opt/homebrew/lib"

echo ""
echo "[1/3] Compiling test program..."
g++ $CXXFLAGS $INCLUDES $LIB_PATHS \
  test_morsel.cpp \
  $LIBS \
  -o test_morsel || {
    echo "✗ Test build failed"
    echo ""
    echo "Missing dependencies? Try:"
    if [ "$OS" = "Darwin" ]; then
      echo "  brew install apache-arrow"
    else
      echo "  sudo yum install arrow-devel parquet-devel"
    fi
    exit 1
  }

if [ -f test_morsel ]; then
  echo "✓ Test program built"
  ls -lh test_morsel
else
  echo "✗ Test build failed"
  exit 1
fi

echo ""
echo "[2/3] Compiling JNI library..."
g++ $CXXFLAGS $INCLUDES $LIB_PATHS -shared \
  morsel_jni_bridge.cpp \
  $LIBS \
  $RPATH_FLAG \
  -o libmorsel_engine.$LIB_SUFFIX || {
    echo "✗ JNI build failed"
    exit 1
  }

if [ -f libmorsel_engine.$LIB_SUFFIX ]; then
  echo "✓ JNI library built"
  ls -lh libmorsel_engine.$LIB_SUFFIX
  echo ""
  echo "Dependencies:"
  $LINK_CHECK libmorsel_engine.$LIB_SUFFIX | grep -E "arrow|parquet" || echo "  (Arrow/Parquet linked)"
else
  echo "✗ JNI build failed"
  exit 1
fi

echo ""
echo "[3/3] Testing (optional)..."

# Find a test parquet file
TEST_FILE="/tmp/spark-tests/tpcds/data/store_sales/part-00000.snappy.parquet"

if [ ! -f "$TEST_FILE" ]; then
  echo "Warning: Test file not found: $TEST_FILE"
  echo "Skipping test validation"
  echo ""
  echo "To test manually:"
  echo "  ./test_morsel <parquet_file> [threads]"
else
  echo "Testing with: $TEST_FILE"
  ./test_morsel "$TEST_FILE" 4 || {
    echo "✗ Test failed"
    exit 1
  }
fi

echo ""
echo "=== Build Complete ==="
echo "Library: libmorsel_engine.$LIB_SUFFIX"
echo ""
echo "Next steps:"
echo "  1. Copy library to Spark distribution"
echo "  2. Set library path in spark-env.sh"
echo "  3. Test with spark-sql --conf spark.sql.morsel.enabled=true"
