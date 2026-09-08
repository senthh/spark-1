#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Pin and build Velox on CentOS 8 (node 160), then libvegam.
# Uses gcc-toolset-12. No libhdfs. Bundled third-party deps.

set -euo pipefail

VELOX_HOME="${VELOX_HOME:-/home/acceldata/velox-src/velox}"
VEGAM_NATIVE="${VEGAM_NATIVE:-/tmp/vegam-native}"
BUILD_DIR="${VELOX_HOME}/_build"
JOBS="${JOBS:-$(nproc)}"

source /opt/rh/gcc-toolset-12/enable
export CC=gcc CXX=g++
export VELOX_DEPENDENCY_SOURCE=BUNDLED

echo "g++=$(g++ --version | head -1)"
echo "VELOX_HOME=${VELOX_HOME}"
git -C "${VELOX_HOME}" rev-parse --short HEAD
git -C "${VELOX_HOME}" log -1 --oneline

mkdir -p "${BUILD_DIR}"
cmake -S "${VELOX_HOME}" -B "${BUILD_DIR}" -GNinja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_STANDARD=20 \
  -DVELOX_BUILD_TESTING=OFF \
  -DVELOX_BUILD_TEST_UTILS=OFF \
  -DVELOX_ENABLE_EXAMPLES=OFF \
  -DVELOX_ENABLE_BENCHMARKS=OFF \
  -DVELOX_ENABLE_BENCHMARKS_BASIC=OFF \
  -DVELOX_ENABLE_PARQUET=ON \
  -DVELOX_ENABLE_HDFS=OFF \
  -DVELOX_ENABLE_S3=OFF \
  -DVELOX_ENABLE_GEO=OFF \
  -DVELOX_ENABLE_TPCH_CONNECTOR=OFF \
  -DVELOX_ENABLE_TPCDS_CONNECTOR=OFF \
  -DVELOX_ENABLE_ICEBERG_FUNCTIONS=OFF \
  -DVELOX_ENABLE_FAISS=OFF \
  -DVELOX_MONO_LIBRARY=ON \
  -DVELOX_BUILD_SHARED=ON \
  -DVELOX_DEPENDENCY_SOURCE=BUNDLED

cmake --build "${BUILD_DIR}" --target velox -j "${JOBS}"

export VELOX_HOME
export VELOX_LIB="${BUILD_DIR}/libvelox.so"
if [ ! -f "${VELOX_LIB}" ]; then
  echo "libvelox.so missing after build" >&2
  ls -l "${BUILD_DIR}"/*.so || true
  exit 1
fi

mkdir -p "${VEGAM_NATIVE}"
rsync -a --delete \
  --exclude build \
  "$(dirname "$0")/../src/main/native/" "${VEGAM_NATIVE}/src/" 2>/dev/null || true

# Caller copies native sources into VEGAM_NATIVE if rsync source is absent.
if [ -f "${VEGAM_NATIVE}/CMakeLists.txt" ]; then
  SRC="${VEGAM_NATIVE}"
elif [ -f "${VEGAM_NATIVE}/src/CMakeLists.txt" ]; then
  SRC="${VEGAM_NATIVE}/src"
else
  SRC="$(cd "$(dirname "$0")/../src/main/native" && pwd)"
fi

cmake -S "${SRC}" -B "${VEGAM_NATIVE}/build"
cmake --build "${VEGAM_NATIVE}/build" -j "${JOBS}"
echo "built ${VEGAM_NATIVE}/build/libvegam.so"
ls -lh "${VELOX_LIB}" "${VEGAM_NATIVE}/build/libvegam.so"
