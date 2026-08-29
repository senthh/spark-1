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

#include "nativesql.h"

#include <jni.h>
#include <cstdlib>
#include <vector>

static NsType java_array_type(JNIEnv *env, jobject arr) {
  if (arr == nullptr) return NS_I64;
  jclass cls = env->GetObjectClass(arr);
  if (env->IsInstanceOf(arr, env->FindClass("[I"))) return NS_I32;
  if (env->IsInstanceOf(arr, env->FindClass("[J"))) return NS_I64;
  if (env->IsInstanceOf(arr, env->FindClass("[D"))) return NS_F64;
  if (env->IsInstanceOf(arr, env->FindClass("[Z"))) return NS_BOOL;
  (void)cls;
  return NS_I64;
}

extern "C" JNIEXPORT jobjectArray JNICALL
Java_org_apache_spark_sql_execution_nativesql_NativeSqlJni_execute(
    JNIEnv *env, jclass, jstring planIr, jobjectArray columns, jintArray numRows) {
  const char *ir = env->GetStringUTFChars(planIr, nullptr);
  const jsize n_in = columns ? env->GetArrayLength(columns) : 0;
  jint *nrow = numRows ? env->GetIntArrayElements(numRows, nullptr) : nullptr;

  std::vector<NsBatch> batches(static_cast<size_t>(n_in));
  std::vector<std::vector<NsCol>> colstore(static_cast<size_t>(n_in));

  for (jsize i = 0; i < n_in; ++i) {
    jobjectArray batch = reinterpret_cast<jobjectArray>(env->GetObjectArrayElement(columns, i));
    const int rows = nrow ? nrow[i] : 0;
    const jsize ncols = batch ? env->GetArrayLength(batch) : 0;
    colstore[i].resize(static_cast<size_t>(ncols));
    batches[i].n_cols = static_cast<int32_t>(ncols);
    batches[i].n_rows = rows;
    batches[i].cols = ncols ? colstore[i].data() : nullptr;
    for (jsize c = 0; c < ncols; ++c) {
      jobject arr = env->GetObjectArrayElement(batch, c);
      NsCol &col = colstore[i][c];
      col.n_rows = rows;
      col.type = java_array_type(env, arr);
      col.data = nullptr;
      if (arr == nullptr) continue;
      if (col.type == NS_I32) {
        col.data = env->GetIntArrayElements(reinterpret_cast<jintArray>(arr), nullptr);
      } else if (col.type == NS_I64) {
        col.data = env->GetLongArrayElements(reinterpret_cast<jlongArray>(arr), nullptr);
      } else if (col.type == NS_F64) {
        col.data = env->GetDoubleArrayElements(reinterpret_cast<jdoubleArray>(arr), nullptr);
      } else {
        col.data = env->GetBooleanArrayElements(reinterpret_cast<jbooleanArray>(arr), nullptr);
      }
    }
  }

  NsBatch out{};
  const int rc = ns_execute(ir, batches.data(), static_cast<int>(n_in), &out);
  env->ReleaseStringUTFChars(planIr, ir);

  /* release input pins */
  for (jsize i = 0; i < n_in; ++i) {
    jobjectArray batch = reinterpret_cast<jobjectArray>(env->GetObjectArrayElement(columns, i));
    const jsize ncols = batch ? env->GetArrayLength(batch) : 0;
    for (jsize c = 0; c < ncols; ++c) {
      jobject arr = env->GetObjectArrayElement(batch, c);
      if (arr == nullptr) continue;
      NsCol &col = colstore[i][c];
      if (col.type == NS_I32) {
        env->ReleaseIntArrayElements(reinterpret_cast<jintArray>(arr),
                                     static_cast<jint *>(col.data), JNI_ABORT);
      } else if (col.type == NS_I64) {
        env->ReleaseLongArrayElements(reinterpret_cast<jlongArray>(arr),
                                      static_cast<jlong *>(col.data), JNI_ABORT);
      } else if (col.type == NS_F64) {
        env->ReleaseDoubleArrayElements(reinterpret_cast<jdoubleArray>(arr),
                                        static_cast<jdouble *>(col.data), JNI_ABORT);
      } else {
        env->ReleaseBooleanArrayElements(reinterpret_cast<jbooleanArray>(arr),
                                         static_cast<jboolean *>(col.data), JNI_ABORT);
      }
    }
  }
  if (nrow) env->ReleaseIntArrayElements(numRows, nrow, JNI_ABORT);

  if (rc != 0) {
    jclass ex = env->FindClass("java/lang/RuntimeException");
    env->ThrowNew(ex, "Native SQL execute failed");
    return nullptr;
  }

  jobjectArray result = env->NewObjectArray(2, env->FindClass("java/lang/Object"), nullptr);
  jobject nobj = env->NewObject(env->FindClass("java/lang/Integer"),
                                env->GetMethodID(env->FindClass("java/lang/Integer"),
                                                 "<init>", "(I)V"),
                                out.n_rows);
  env->SetObjectArrayElement(result, 0, nobj);

  jobjectArray cols = env->NewObjectArray(out.n_cols, env->FindClass("java/lang/Object"), nullptr);
  for (int c = 0; c < out.n_cols; ++c) {
    const NsCol &col = out.cols[c];
    if (col.type == NS_I32 || col.type == NS_I64) {
      jlongArray a = env->NewLongArray(out.n_rows);
      if (out.n_rows) {
        env->SetLongArrayRegion(a, 0, out.n_rows, static_cast<const jlong *>(col.data));
      }
      env->SetObjectArrayElement(cols, c, a);
    } else if (col.type == NS_F64) {
      jdoubleArray a = env->NewDoubleArray(out.n_rows);
      if (out.n_rows) {
        env->SetDoubleArrayRegion(a, 0, out.n_rows, static_cast<const jdouble *>(col.data));
      }
      env->SetObjectArrayElement(cols, c, a);
    } else {
      jbooleanArray a = env->NewBooleanArray(out.n_rows);
      if (out.n_rows) {
        env->SetBooleanArrayRegion(a, 0, out.n_rows, static_cast<const jboolean *>(col.data));
      }
      env->SetObjectArrayElement(cols, c, a);
    }
  }
  env->SetObjectArrayElement(result, 1, cols);
  ns_batch_free(&out);
  return result;
}
