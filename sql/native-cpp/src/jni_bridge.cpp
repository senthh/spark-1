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
#include "parquet_scan.h"

#include <jni.h>
#include <cstdint>
#include <cstdlib>
#include <mutex>
#include <string>
#include <vector>

static thread_local JNIEnv *g_hdfs_env = nullptr;
static jclass g_jni_cls = nullptr;
static jmethodID g_mid_hdfs_size = nullptr;
static jmethodID g_mid_hdfs_pread = nullptr;
static std::once_flag g_jni_once;

static bool jni_ok(JNIEnv *env) { return env != nullptr && !env->ExceptionCheck(); }

static int64_t java_hdfs_size(const char *uri) {
  JNIEnv *env = g_hdfs_env;
  if (env == nullptr || g_jni_cls == nullptr || uri == nullptr) return -1;
  jmethodID mid = g_mid_hdfs_size;
  if (mid == nullptr) {
    mid = env->GetStaticMethodID(g_jni_cls, "hdfsSize", "(Ljava/lang/String;)J");
  }
  if (mid == nullptr || env->ExceptionCheck()) {
    env->ExceptionClear();
    return -1;
  }
  jstring juri = env->NewStringUTF(uri);
  if (juri == nullptr) return -1;
  jlong sz = env->CallStaticLongMethod(g_jni_cls, mid, juri);
  env->DeleteLocalRef(juri);
  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    return -1;
  }
  return static_cast<int64_t>(sz);
}

static int64_t java_hdfs_pread(const char *uri, int64_t off, void *buf, int64_t n) {
  JNIEnv *env = g_hdfs_env;
  if (env == nullptr || g_jni_cls == nullptr || uri == nullptr || buf == nullptr) {
    return -1;
  }
  if (n <= 0) return 0;
  jmethodID mid = g_mid_hdfs_pread;
  if (mid == nullptr) {
    mid = env->GetStaticMethodID(g_jni_cls, "hdfsPread",
                                 "(Ljava/lang/String;J[BI)I");
  }
  if (mid == nullptr || env->ExceptionCheck()) {
    env->ExceptionClear();
    return -1;
  }
  if (env->PushLocalFrame(8) != 0) {
    env->ExceptionClear();
    return -1;
  }
  const jint want = n > static_cast<int64_t>(INT32_MAX) ? INT32_MAX : static_cast<jint>(n);
  jstring juri = env->NewStringUTF(uri);
  jbyteArray jbuf = env->NewByteArray(want);
  if (juri == nullptr || jbuf == nullptr) {
    env->PopLocalFrame(nullptr);
    return -1;
  }
  jint got = env->CallStaticIntMethod(g_jni_cls, mid, juri, static_cast<jlong>(off),
                                      jbuf, want);
  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
    env->PopLocalFrame(nullptr);
    return -1;
  }
  if (got > 0) {
    env->GetByteArrayRegion(jbuf, 0, got, reinterpret_cast<jbyte *>(buf));
  }
  env->PopLocalFrame(nullptr);
  return static_cast<int64_t>(got);
}

/* Pack raw parquet bytes as byte[]. NewStringUTF aborts the JVM on
 * non-modified-UTF-8 (CHAR/BINARY/decimal leftovers). */
static jbyteArray jbytes(JNIEnv *env, const char *s, int32_t len) {
  if (len < 0) len = 0;
  jbyteArray a = env->NewByteArray(len);
  if (a == nullptr || env->ExceptionCheck()) return nullptr;
  if (len > 0 && s != nullptr) {
    env->SetByteArrayRegion(a, 0, len, reinterpret_cast<const jbyte *>(s));
  }
  return a;
}

static bool is_class(JNIEnv *env, jobject obj, const char *name) {
  if (obj == nullptr) return false;
  jclass cls = env->FindClass(name);
  if (cls == nullptr) {
    env->ExceptionClear();
    return false;
  }
  const bool ok = env->IsInstanceOf(obj, cls);
  env->DeleteLocalRef(cls);
  return ok;
}

static bool primitive_type(JNIEnv *env, jobject arr, NsType *t) {
  if (is_class(env, arr, "[I")) {
    *t = NS_I32;
    return true;
  }
  if (is_class(env, arr, "[J")) {
    *t = NS_I64;
    return true;
  }
  if (is_class(env, arr, "[D")) {
    *t = NS_F64;
    return true;
  }
  if (is_class(env, arr, "[Z")) {
    *t = NS_BOOL;
    return true;
  }
  return false;
}

static void *pin_primitive(JNIEnv *env, jobject arr, NsType t) {
  if (arr == nullptr) return nullptr;
  if (t == NS_I32) {
    return env->GetIntArrayElements(reinterpret_cast<jintArray>(arr), nullptr);
  }
  if (t == NS_I64) {
    return env->GetLongArrayElements(reinterpret_cast<jlongArray>(arr), nullptr);
  }
  if (t == NS_F64) {
    return env->GetDoubleArrayElements(reinterpret_cast<jdoubleArray>(arr), nullptr);
  }
  if (t == NS_BOOL) {
    return env->GetBooleanArrayElements(reinterpret_cast<jbooleanArray>(arr), nullptr);
  }
  return nullptr;
}

static void unpin_primitive(JNIEnv *env, jobject arr, NsType t, void *data) {
  if (arr == nullptr || data == nullptr) return;
  if (t == NS_I32) {
    env->ReleaseIntArrayElements(reinterpret_cast<jintArray>(arr),
                                 static_cast<jint *>(data), JNI_ABORT);
  } else if (t == NS_I64) {
    env->ReleaseLongArrayElements(reinterpret_cast<jlongArray>(arr),
                                  static_cast<jlong *>(data), JNI_ABORT);
  } else if (t == NS_F64) {
    env->ReleaseDoubleArrayElements(reinterpret_cast<jdoubleArray>(arr),
                                    static_cast<jdouble *>(data), JNI_ABORT);
  } else if (t == NS_BOOL) {
    env->ReleaseBooleanArrayElements(reinterpret_cast<jbooleanArray>(arr),
                                     static_cast<jboolean *>(data), JNI_ABORT);
  }
}

static void fill_col(JNIEnv *env, NsCol *col, jobject arr, int rows) {
  col->data = nullptr;
  col->n_rows = 0;
  col->type = NS_I64;
  NsType t;
  if (!primitive_type(env, arr, &t)) return;
  const jsize alen = env->GetArrayLength(reinterpret_cast<jarray>(arr));
  col->type = t;
  if (rows > 0 && alen > rows) {
    col->n_rows = rows;
  } else {
    col->n_rows = static_cast<int32_t>(alen);
  }
  if (col->n_rows < 0) col->n_rows = 0;
  col->data = pin_primitive(env, arr, t);
}

extern "C" JNIEXPORT jobjectArray JNICALL
Java_org_apache_spark_sql_execution_nativesql_NativeSqlJni_execute(
    JNIEnv *env, jclass, jstring planIr, jobjectArray columns, jintArray numRows) {
  g_hdfs_env = env;
  const char *ir = env->GetStringUTFChars(planIr, nullptr);
  const jsize n_in = columns ? env->GetArrayLength(columns) : 0;
  jint *nrow = numRows ? env->GetIntArrayElements(numRows, nullptr) : nullptr;

  std::vector<NsBatch> batches(static_cast<size_t>(n_in));
  std::vector<std::vector<NsCol>> colstore(static_cast<size_t>(n_in));
  std::vector<std::vector<jobject>> pinned(static_cast<size_t>(n_in));

  for (jsize i = 0; i < n_in; ++i) {
    jobject batchObj = env->GetObjectArrayElement(columns, i);
    int rows = nrow ? nrow[i] : 0;
    if (rows < 0) rows = 0;
    NsType prim;
    if (primitive_type(env, batchObj, &prim)) {
      /* A bare int[]/long[] was passed as the batch. Treat as one column. */
      colstore[i].resize(1);
      pinned[i].resize(1);
      fill_col(env, &colstore[i][0], batchObj, rows);
      pinned[i][0] = batchObj;
      batches[i].n_cols = 1;
      batches[i].n_rows = colstore[i][0].n_rows;
      batches[i].cols = colstore[i].data();
      continue;
    }
    if (!is_class(env, batchObj, "[Ljava/lang/Object;")) {
      batches[i].n_cols = 0;
      batches[i].n_rows = rows;
      batches[i].cols = nullptr;
      continue;
    }
    jobjectArray batch = reinterpret_cast<jobjectArray>(batchObj);
    const jsize ncols = env->GetArrayLength(batch);
    colstore[i].resize(static_cast<size_t>(ncols));
    pinned[i].resize(static_cast<size_t>(ncols));
    batches[i].n_cols = static_cast<int32_t>(ncols);
    batches[i].cols = ncols ? colstore[i].data() : nullptr;
    int cap = rows;
    for (jsize c = 0; c < ncols; ++c) {
      jobject arr = env->GetObjectArrayElement(batch, c);
      pinned[i][c] = arr;
      fill_col(env, &colstore[i][c], arr, rows);
      if (arr != nullptr && colstore[i][c].n_rows < cap) {
        cap = colstore[i][c].n_rows;
      }
    }
    batches[i].n_rows = ncols == 0 ? rows : cap;
  }

  NsBatch out{};
  const int rc = ns_execute(ir, batches.data(), static_cast<int>(n_in), &out);
  env->ReleaseStringUTFChars(planIr, ir);

  for (jsize i = 0; i < n_in; ++i) {
    for (size_t c = 0; c < pinned[i].size(); ++c) {
      unpin_primitive(env, pinned[i][c], colstore[i][c].type, colstore[i][c].data);
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
    if (col.data == nullptr || out.n_rows <= 0) {
      if (col.type == NS_F64) {
        env->SetObjectArrayElement(cols, c, env->NewDoubleArray(out.n_rows));
      } else if (col.type == NS_BOOL) {
        env->SetObjectArrayElement(cols, c, env->NewBooleanArray(out.n_rows));
      } else {
        env->SetObjectArrayElement(cols, c, env->NewLongArray(out.n_rows));
      }
      continue;
    }
    if (col.type == NS_I32 || col.type == NS_I64) {
      jlongArray a = env->NewLongArray(out.n_rows);
      env->SetLongArrayRegion(a, 0, out.n_rows, static_cast<const jlong *>(col.data));
      env->SetObjectArrayElement(cols, c, a);
    } else if (col.type == NS_F64) {
      jdoubleArray a = env->NewDoubleArray(out.n_rows);
      env->SetDoubleArrayRegion(a, 0, out.n_rows, static_cast<const jdouble *>(col.data));
      env->SetObjectArrayElement(cols, c, a);
    } else {
      jbooleanArray a = env->NewBooleanArray(out.n_rows);
      env->SetBooleanArrayRegion(a, 0, out.n_rows, static_cast<const jboolean *>(col.data));
      env->SetObjectArrayElement(cols, c, a);
    }
  }
  env->SetObjectArrayElement(result, 1, cols);
  ns_batch_free(&out);
  return result;
}

static jobjectArray pack_result(JNIEnv *env, NsBatch *out) {
  const int nrows = out->n_rows < 0 ? 0 : out->n_rows;
  jobjectArray result = env->NewObjectArray(4, env->FindClass("java/lang/Object"), nullptr);
  if (!jni_ok(env) || result == nullptr) {
    ns_batch_free(out);
    ns_strdict_clear();
    return nullptr;
  }
  jclass icls = env->FindClass("java/lang/Integer");
  jmethodID ictor = icls ? env->GetMethodID(icls, "<init>", "(I)V") : nullptr;
  jobject nobj = (icls && ictor) ? env->NewObject(icls, ictor, nrows) : nullptr;
  if (!jni_ok(env) || nobj == nullptr) {
    ns_batch_free(out);
    ns_strdict_clear();
    return nullptr;
  }
  env->SetObjectArrayElement(result, 0, nobj);
  jobjectArray cols = env->NewObjectArray(out->n_cols, env->FindClass("java/lang/Object"), nullptr);
  if (!jni_ok(env) || cols == nullptr) {
    ns_batch_free(out);
    ns_strdict_clear();
    return nullptr;
  }
  for (int c = 0; c < out->n_cols; ++c) {
    const NsCol &col = out->cols[c];
    jobject arr = nullptr;
    if (col.data == nullptr || nrows <= 0) {
      arr = env->NewLongArray(nrows);
    } else if (col.type == NS_F64) {
      jdoubleArray a = env->NewDoubleArray(nrows);
      if (a && col.data) {
        env->SetDoubleArrayRegion(a, 0, nrows, static_cast<const jdouble *>(col.data));
      }
      arr = a;
    } else if (col.type == NS_BOOL) {
      jbooleanArray a = env->NewBooleanArray(nrows);
      if (a && col.data) {
        env->SetBooleanArrayRegion(a, 0, nrows, static_cast<const jboolean *>(col.data));
      }
      arr = a;
    } else {
      jlongArray a = env->NewLongArray(nrows);
      if (a && col.data) {
        env->SetLongArrayRegion(a, 0, nrows, static_cast<const jlong *>(col.data));
      }
      arr = a;
    }
    if (!jni_ok(env) || arr == nullptr) {
      ns_batch_free(out);
      ns_strdict_clear();
      return nullptr;
    }
    env->SetObjectArrayElement(cols, c, arr);
  }
  env->SetObjectArrayElement(result, 1, cols);
  const int nd = ns_strdict_size();
  if (nd > 16 && env->EnsureLocalCapacity(32) != 0) {
    ns_batch_free(out);
    ns_strdict_clear();
    return nullptr;
  }
  jlongArray hashes = env->NewLongArray(nd);
  jobjectArray vals = env->NewObjectArray(nd, env->FindClass("[B"), nullptr);
  if (!jni_ok(env) || hashes == nullptr || vals == nullptr) {
    ns_batch_free(out);
    ns_strdict_clear();
    return nullptr;
  }
  for (int i = 0; i < nd; ++i) {
    int64_t h = 0;
    const char *s = nullptr;
    int32_t len = 0;
    ns_strdict_at(i, &h, &s, &len);
    env->SetLongArrayRegion(hashes, i, 1, reinterpret_cast<const jlong *>(&h));
    jbyteArray ba = jbytes(env, s, len);
    if (!jni_ok(env)) {
      ns_batch_free(out);
      ns_strdict_clear();
      return nullptr;
    }
    if (ba != nullptr) {
      env->SetObjectArrayElement(vals, i, ba);
      env->DeleteLocalRef(ba);
    }
  }
  env->SetObjectArrayElement(result, 2, hashes);
  env->SetObjectArrayElement(result, 3, vals);
  ns_batch_free(out);
  ns_strdict_clear();
  return result;
}

static void init_jni_hdfs(JNIEnv *env) {
  jclass local =
      env->FindClass("org/apache/spark/sql/execution/nativesql/NativeSqlJni");
  if (local == nullptr) return;
  g_jni_cls = reinterpret_cast<jclass>(env->NewGlobalRef(local));
  env->DeleteLocalRef(local);
  g_mid_hdfs_size =
      env->GetStaticMethodID(g_jni_cls, "hdfsSize", "(Ljava/lang/String;)J");
  g_mid_hdfs_pread = env->GetStaticMethodID(
      g_jni_cls, "hdfsPread", "(Ljava/lang/String;J[BI)I");
}

extern "C" JNIEXPORT jobjectArray JNICALL
Java_org_apache_spark_sql_execution_nativesql_NativeSqlJni_executeScan(
    JNIEnv *env, jclass, jstring planIr, jobjectArray columns, jintArray numRows,
    jobjectArray scans) {
  g_hdfs_env = env;
  std::call_once(g_jni_once, init_jni_hdfs, env);
  ns_parquet_set_hdfs_io(java_hdfs_size, java_hdfs_pread);
  ns_strdict_clear();
  const char *ir = env->GetStringUTFChars(planIr, nullptr);
  const jsize n_in = columns ? env->GetArrayLength(columns) : 0;
  jint *nrow = numRows ? env->GetIntArrayElements(numRows, nullptr) : nullptr;

  std::vector<NsBatch> batches(static_cast<size_t>(n_in));
  std::vector<std::vector<NsCol>> colstore(static_cast<size_t>(n_in));
  std::vector<NsFileScan> fscans(static_cast<size_t>(n_in));
  std::vector<std::vector<NsFileSplit>> splits(static_cast<size_t>(n_in));
  std::vector<std::vector<const char *>> names(static_cast<size_t>(n_in));
  std::vector<std::vector<int32_t>> types(static_cast<size_t>(n_in));
  std::vector<std::vector<std::string>> name_store(static_cast<size_t>(n_in));
  std::vector<std::vector<std::string>> path_store(static_cast<size_t>(n_in));
  std::vector<std::vector<std::vector<uint8_t>>> blob_store(static_cast<size_t>(n_in));
  std::vector<std::vector<NsColPred>> pred_store(static_cast<size_t>(n_in));

  for (jsize i = 0; i < n_in; ++i) {
    batches[i].n_cols = 0;
    batches[i].n_rows = nrow ? nrow[i] : 0;
    batches[i].cols = nullptr;
    fscans[i].n_splits = 0;
    fscans[i].splits = nullptr;
    if (!scans) continue;
    jobject spec = env->GetObjectArrayElement(scans, i);
    if (spec == nullptr || !is_class(env, spec, "[Ljava/lang/Object;")) continue;
    jobjectArray sp = reinterpret_cast<jobjectArray>(spec);
    if (env->GetArrayLength(sp) < 6) continue;
    jobject pathsObj = env->GetObjectArrayElement(sp, 0);
    jobject startsObj = env->GetObjectArrayElement(sp, 1);
    jobject lensObj = env->GetObjectArrayElement(sp, 2);
    jobject blobsObj = env->GetObjectArrayElement(sp, 3);
    jobject namesObj = env->GetObjectArrayElement(sp, 4);
    jobject typesObj = env->GetObjectArrayElement(sp, 5);
    jobjectArray pathArr = reinterpret_cast<jobjectArray>(pathsObj);
    jlongArray startArr = reinterpret_cast<jlongArray>(startsObj);
    jlongArray lenArr = reinterpret_cast<jlongArray>(lensObj);
    jobjectArray blobArr = reinterpret_cast<jobjectArray>(blobsObj);
    jobjectArray nameArr = reinterpret_cast<jobjectArray>(namesObj);
    jintArray typeArr = reinterpret_cast<jintArray>(typesObj);
    const jsize nf = pathArr ? env->GetArrayLength(pathArr) : 0;
    const jsize nc = nameArr ? env->GetArrayLength(nameArr) : 0;
    splits[i].resize(static_cast<size_t>(nf));
    path_store[i].resize(static_cast<size_t>(nf));
    blob_store[i].resize(static_cast<size_t>(nf));
    jlong *starts = startArr ? env->GetLongArrayElements(startArr, nullptr) : nullptr;
    jlong *lens = lenArr ? env->GetLongArrayElements(lenArr, nullptr) : nullptr;
    for (jsize f = 0; f < nf; ++f) {
      jstring jp = pathArr ? reinterpret_cast<jstring>(env->GetObjectArrayElement(pathArr, f))
                           : nullptr;
      if (jp) {
        const char *cs = env->GetStringUTFChars(jp, nullptr);
        path_store[i][static_cast<size_t>(f)] = cs ? cs : "";
        env->ReleaseStringUTFChars(jp, cs);
      }
      splits[i][static_cast<size_t>(f)].path = path_store[i][static_cast<size_t>(f)].c_str();
      jobject blob = blobArr ? env->GetObjectArrayElement(blobArr, f) : nullptr;
      if (blob && is_class(env, blob, "[B")) {
        jbyteArray ba = reinterpret_cast<jbyteArray>(blob);
        jsize blen = env->GetArrayLength(ba);
        jbyte *bp = env->GetByteArrayElements(ba, nullptr);
        if (bp != nullptr && blen > 0) {
          blob_store[i][static_cast<size_t>(f)].assign(
              reinterpret_cast<const uint8_t *>(bp),
              reinterpret_cast<const uint8_t *>(bp) + static_cast<size_t>(blen));
          splits[i][static_cast<size_t>(f)].bytes =
              blob_store[i][static_cast<size_t>(f)].data();
          splits[i][static_cast<size_t>(f)].nbytes = blen;
          splits[i][static_cast<size_t>(f)].path = nullptr;
        }
        if (bp != nullptr) {
          env->ReleaseByteArrayElements(ba, bp, JNI_ABORT);
        }
      }
      splits[i][static_cast<size_t>(f)].start = starts ? starts[f] : 0;
      splits[i][static_cast<size_t>(f)].length = lens ? lens[f] : 0;
    }
    name_store[i].resize(static_cast<size_t>(nc));
    names[i].resize(static_cast<size_t>(nc));
    types[i].resize(static_cast<size_t>(nc));
    jint *tp = typeArr ? env->GetIntArrayElements(typeArr, nullptr) : nullptr;
    for (jsize c = 0; c < nc; ++c) {
      jstring jn = nameArr ? reinterpret_cast<jstring>(env->GetObjectArrayElement(nameArr, c))
                           : nullptr;
      if (jn) {
        const char *cs = env->GetStringUTFChars(jn, nullptr);
        name_store[i][static_cast<size_t>(c)] = cs ? cs : "";
        env->ReleaseStringUTFChars(jn, cs);
      }
      names[i][static_cast<size_t>(c)] = name_store[i][static_cast<size_t>(c)].c_str();
      types[i][static_cast<size_t>(c)] = tp ? tp[c] : NS_I64;
    }
    if (tp) env->ReleaseIntArrayElements(typeArr, tp, JNI_ABORT);
    if (starts) env->ReleaseLongArrayElements(startArr, starts, JNI_ABORT);
    if (lens) env->ReleaseLongArrayElements(lenArr, lens, JNI_ABORT);
    fscans[i].splits = nf ? splits[i].data() : nullptr;
    fscans[i].n_splits = static_cast<int32_t>(nf);
    fscans[i].col_names = nc ? names[i].data() : nullptr;
    fscans[i].col_types = nc ? types[i].data() : nullptr;
    fscans[i].n_cols = static_cast<int32_t>(nc);
    fscans[i].preds = nullptr;
    fscans[i].n_preds = 0;
    if (env->GetArrayLength(sp) >= 7) {
      jobject predsObj = env->GetObjectArrayElement(sp, 6);
      if (predsObj && is_class(env, predsObj, "[Ljava/lang/Object;")) {
        jobjectArray pa = reinterpret_cast<jobjectArray>(predsObj);
        if (env->GetArrayLength(pa) >= 3) {
          jintArray colArr = reinterpret_cast<jintArray>(env->GetObjectArrayElement(pa, 0));
          jintArray opArr = reinterpret_cast<jintArray>(env->GetObjectArrayElement(pa, 1));
          jlongArray valArr = reinterpret_cast<jlongArray>(env->GetObjectArrayElement(pa, 2));
          const jsize np = colArr ? env->GetArrayLength(colArr) : 0;
          jint *pc = colArr ? env->GetIntArrayElements(colArr, nullptr) : nullptr;
          jint *po = opArr ? env->GetIntArrayElements(opArr, nullptr) : nullptr;
          jlong *pv = valArr ? env->GetLongArrayElements(valArr, nullptr) : nullptr;
          pred_store[i].resize(static_cast<size_t>(np));
          for (jsize k = 0; k < np; ++k) {
            pred_store[i][static_cast<size_t>(k)].col = pc ? pc[k] : -1;
            pred_store[i][static_cast<size_t>(k)].op = po ? po[k] : 0;
            pred_store[i][static_cast<size_t>(k)].value = pv ? pv[k] : 0;
          }
          if (pc) env->ReleaseIntArrayElements(colArr, pc, JNI_ABORT);
          if (po) env->ReleaseIntArrayElements(opArr, po, JNI_ABORT);
          if (pv) env->ReleaseLongArrayElements(valArr, pv, JNI_ABORT);
          if (np > 0) {
            fscans[i].preds = pred_store[i].data();
            fscans[i].n_preds = static_cast<int32_t>(np);
          }
        }
      }
    }
  }

  /* Also accept in-memory batches for leaves that are not file scans. */
  std::vector<std::vector<jobject>> pinned(static_cast<size_t>(n_in));
  for (jsize i = 0; i < n_in; ++i) {
    if (fscans[i].n_splits > 0) continue;
    jobject batchObj = columns ? env->GetObjectArrayElement(columns, i) : nullptr;
    int rows = nrow ? nrow[i] : 0;
    if (batchObj == nullptr) continue;
    if (!is_class(env, batchObj, "[Ljava/lang/Object;")) continue;
    jobjectArray batch = reinterpret_cast<jobjectArray>(batchObj);
    const jsize ncols = env->GetArrayLength(batch);
    colstore[i].resize(static_cast<size_t>(ncols));
    pinned[i].resize(static_cast<size_t>(ncols));
    batches[i].n_cols = static_cast<int32_t>(ncols);
    batches[i].cols = ncols ? colstore[i].data() : nullptr;
    int cap = rows;
    for (jsize c = 0; c < ncols; ++c) {
      jobject arr = env->GetObjectArrayElement(batch, c);
      pinned[i][c] = arr;
      fill_col(env, &colstore[i][c], arr, rows);
      if (arr != nullptr && colstore[i][c].n_rows < cap) cap = colstore[i][c].n_rows;
    }
    batches[i].n_rows = ncols == 0 ? rows : cap;
  }

  NsBatch out{};
  const int rc = ns_execute_scan(ir, batches.data(), fscans.data(), static_cast<int>(n_in), &out);
  env->ReleaseStringUTFChars(planIr, ir);
  for (jsize i = 0; i < n_in; ++i) {
    for (size_t c = 0; c < pinned[i].size(); ++c) {
      unpin_primitive(env, pinned[i][c], colstore[i][c].type, colstore[i][c].data);
    }
  }
  if (nrow) env->ReleaseIntArrayElements(numRows, nrow, JNI_ABORT);
  if (rc != 0) {
    jclass ex = env->FindClass("java/lang/RuntimeException");
    env->ThrowNew(ex, "Native SQL parquet execute failed (hdfs range/scan)");
    return nullptr;
  }
  return pack_result(env, &out);
}
