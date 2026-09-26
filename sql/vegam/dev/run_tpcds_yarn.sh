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

# Run run_tpcds.py on the smile YARN cluster for one engine.
#
#   run_tpcds_yarn.sh vegam  [run_tpcds.py args...]
#   run_tpcds_yarn.sh gluten [run_tpcds.py args...]
#
# Both engines get the same executors, cores and container size. Vegam's
# native memory lives in memoryOverhead; Gluten's in off-heap memory.

set -euo pipefail
ENGINE="${1:?vegam|gluten}"
shift

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-17-openjdk-17.0.20.1.1-1.1.el8_10.x86_64}"
export PATH="${JAVA_HOME}/bin:${PATH}"
export HADOOP_CONF_DIR=/etc/hadoop/conf
export YARN_CONF_DIR=/etc/hadoop/conf
# The managed warehouse is readable by hive/hdfs only; the cluster is unsecured.
export HADOOP_USER_NAME="${HADOOP_USER_NAME:-hdfs}"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-/usr/bin/python3.11}"
export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-${PYSPARK_PYTHON}}"

HERE="$(cd "$(dirname "$0")" && pwd)"
RUNNER="${RUNNER:-${HERE}/run_tpcds.py}"
VEGAM_HOME="${VEGAM_HOME:-/home/acceldata/vegam/spark-420-vegam}"
VEGAM_LIBS="${VEGAM_LIBS:-/home/acceldata/vegam/vegamlib.tgz}"
GLUTEN_HOME="${GLUTEN_HOME:-/usr/odp/current/spark4-client}"
GLUTEN_DIR=/usr/odp/3.3.6.5-1009/spark4/gluten
GLUTEN_JAR="${GLUTEN_JAR:-${GLUTEN_DIR}/gluten-velox-bundle-spark4.1_2.13-linux_amd64-1.7.0.3.3.6.5.jar}"

EXECUTORS="${EXECUTORS:-6}"
CORES="${CORES:-2}"
HEAP="${HEAP:-1536m}"
NATIVE_MB="${NATIVE_MB:-3072}"

MS_DIR=/usr/odp/current/spark2-client/standalone-metastore
MS_JARS="$(ls -1 "${MS_DIR}"/*.jar | sed 's|^|file://|' | paste -sd, -)"

CONFS=(
  --master yarn
  --name "tpcds-${ENGINE}"
  --conf "spark.sql.hive.metastore.jars=path"
  --conf "spark.sql.hive.metastore.jars.path=${MS_JARS}"
  --conf "spark.sql.hive.metastore.version=3.0"
  --conf "spark.yarn.appMasterEnv.JAVA_HOME=${JAVA_HOME}"
  --conf "spark.executorEnv.JAVA_HOME=${JAVA_HOME}"
  --conf spark.dynamicAllocation.enabled=false
  --conf "spark.executor.instances=${EXECUTORS}"
  --conf "spark.executor.cores=${CORES}"
  --conf "spark.executor.memory=${HEAP}"
  --conf spark.driver.memory=2g
  --conf spark.sql.adaptive.enabled=true
  --conf spark.sql.shuffle.partitions=16
  --conf spark.sql.ansi.enabled=false
  # Aggregate-subquery DPP, so no broadcast has to sit under a native stage.
  --conf spark.sql.optimizer.dynamicPartitionPruning.reuseBroadcastOnly=false
)

case "${ENGINE}" in
  vegam)
    SPARK_HOME="${VEGAM_HOME}"
    # The driver must load libvegam too: backend=auto plans native stages
    # only when the library loads on the driver.
    DRIVER_LIBS="${VEGAM_LIBS%.tgz}"
    if [ ! -d "${DRIVER_LIBS}" ] || [ "${VEGAM_LIBS}" -nt "${DRIVER_LIBS}" ]; then
      rm -rf "${DRIVER_LIBS}"
      mkdir -p "${DRIVER_LIBS}"
      tar -xzf "${VEGAM_LIBS}" -C "${DRIVER_LIBS}"
    fi
    CONFS+=(
      --driver-library-path "${DRIVER_LIBS}"
      --conf "spark.driver.extraJavaOptions=-Djava.library.path=${DRIVER_LIBS}"
      --archives "${VEGAM_LIBS}#vegamlib"
      --conf spark.executorEnv.LD_LIBRARY_PATH=./vegamlib
      --conf spark.executor.extraJavaOptions=-Djava.library.path=./vegamlib
      --conf "spark.executor.memoryOverhead=$((NATIVE_MB + 384))m"
    )
    ;;
  gluten)
    SPARK_HOME="${GLUTEN_HOME}"
    # Gluten must share Spark's class loader: with Gluten toggled off its
    # shuffle manager delegates to Spark's package-private sort writers.
    # --jars ships the jar into each container's working directory.
    CONFS+=(
      --jars "${GLUTEN_JAR}"
      --conf "spark.driver.extraClassPath=${GLUTEN_JAR}"
      --conf "spark.executor.extraClassPath=./$(basename "${GLUTEN_JAR}")"
      --conf spark.plugins=org.apache.gluten.GlutenPlugin
      --conf spark.gluten.sql.columnar.backend.lib=velox
      --conf spark.shuffle.manager=org.apache.spark.shuffle.sort.ColumnarShuffleManager
      --conf spark.memory.offHeap.enabled=true
      --conf "spark.memory.offHeap.size=${NATIVE_MB}m"
      --conf spark.executor.memoryOverhead=384m
    )
    ;;
  *)
    echo "unknown engine ${ENGINE}" >&2
    exit 1
    ;;
esac

export SPARK_HOME
echo "engine=${ENGINE} spark=${SPARK_HOME} executors=${EXECUTORS}x${CORES}"
exec "${SPARK_HOME}/bin/spark-submit" "${CONFS[@]}" "${RUNNER}" --engine "${ENGINE}" "$@"
