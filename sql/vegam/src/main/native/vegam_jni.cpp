/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <jni.h>
#include <cstdint>
#include <string>
#include <vector>

#include "vegam_engine.h"

#ifdef VEGAM_HAS_VELOX
#include "vegam_hadoop_file.h"
#endif

/*
 * JNI surface for org.apache.spark.sql.vegam.exec.NativeTask.
 * createTask / nextPage / close only. Operator kernels (C++ or Velox)
 * plug in behind vegam_create_task, not as extra JNI methods.
 *
 * File bytes come from HadoopBytes (Java Hadoop FS). No libhdfs.
 */

extern "C" {

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM* vm, void*) {
#ifdef VEGAM_HAS_VELOX
  vegam_set_javavm(vm);
#endif
  return JNI_VERSION_1_8;
}

JNIEXPORT jlong JNICALL
Java_org_apache_spark_sql_vegam_exec_NativeTask_createTask(
    JNIEnv* env,
    jclass,
    jbyteArray plan_bytes,
    jlong,
    jint threads) {
  if (plan_bytes == nullptr) {
    return 0;
  }
  jsize n = env->GetArrayLength(plan_bytes);
  std::vector<uint8_t> buf(static_cast<size_t>(n));
  env->GetByteArrayRegion(plan_bytes, 0, n, reinterpret_cast<jbyte*>(buf.data()));
  VegamTaskState* task = vegam_create_task(env, buf.data(), n, threads);
  return reinterpret_cast<jlong>(task);
}

JNIEXPORT jint JNICALL
Java_org_apache_spark_sql_vegam_exec_NativeTask_nextPage(
    JNIEnv* env,
    jclass,
    jlong handle,
    jdoubleArray values,
    jbooleanArray nulls,
    jintArray meta) {
  if (handle == 0 || values == nullptr || nulls == nullptr || meta == nullptr) {
    return -1;
  }
  auto* task = reinterpret_cast<VegamTaskState*>(handle);
  jsize cap = env->GetArrayLength(values);
  std::vector<double> v(static_cast<size_t>(cap));
  std::vector<uint8_t> nuls(static_cast<size_t>(cap));
  int m[4] = {0, 0, 0, 0};
  int rows = vegam_next_page(task, v.data(), nuls.data(), m, cap);
  if (rows < 0) {
    return -1;
  }
  env->SetDoubleArrayRegion(values, 0, cap, v.data());
  env->SetBooleanArrayRegion(nulls, 0, cap, reinterpret_cast<const jboolean*>(nuls.data()));
  env->SetIntArrayRegion(meta, 0, 4, m);
  return rows;
}

JNIEXPORT jboolean JNICALL
Java_org_apache_spark_sql_vegam_exec_NativeTask_isArrow(
    JNIEnv*,
    jclass,
    jlong handle) {
  auto* task = reinterpret_cast<VegamTaskState*>(handle);
  return task != nullptr && task->velox != nullptr ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jint JNICALL
Java_org_apache_spark_sql_vegam_exec_NativeTask_nextBatch(
    JNIEnv* env,
    jclass,
    jlong handle,
    jlong arrow_array,
    jlong arrow_schema) {
  auto* task = reinterpret_cast<VegamTaskState*>(handle);
  if (task == nullptr || arrow_array == 0 || arrow_schema == 0) {
    return -1;
  }
#ifdef VEGAM_HAS_VELOX
  if (task->velox == nullptr) {
    return -1;
  }
  try {
    return vegam_velox_next(task->velox, reinterpret_cast<void*>(arrow_array),
                            reinterpret_cast<void*>(arrow_schema));
  } catch (const std::exception& e) {
    if (!env->ExceptionCheck()) {
      jclass rte = env->FindClass("java/lang/RuntimeException");
      env->ThrowNew(rte, (std::string("vegam velox stage failed: ") + e.what()).c_str());
    }
    return -1;
  }
#else
  (void)env;
  return -1;
#endif
}

JNIEXPORT void JNICALL
Java_org_apache_spark_sql_vegam_exec_NativeTask_close(
    JNIEnv*,
    jclass,
    jlong handle) {
  if (handle != 0) {
    vegam_close_task(reinterpret_cast<VegamTaskState*>(handle));
  }
}

}  // extern "C"
