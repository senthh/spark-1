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

#include "vegam_engine.h"
#include "vegam_pipeline.h"
#include "vegam_scheduler.h"

#include <cmath>
#include <cstring>
#include <string>
#include <vector>

namespace {

std::string jstring_to_std(JNIEnv* env, jstring js) {
  if (js == nullptr) {
    return "";
  }
  const char* c = env->GetStringUTFChars(js, nullptr);
  std::string s(c ? c : "");
  if (c) {
    env->ReleaseStringUTFChars(js, c);
  }
  return s;
}

}  // namespace

int VegamTable::col_index(const std::string& name) const {
  for (size_t i = 0; i < names.size(); i++) {
    if (names[i] == name) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

long vegam_footer_rows(JNIEnv* env, const std::string& path) {
  jclass cls = env->FindClass("org/apache/spark/sql/vegam/exec/HadoopBytes");
  if (cls == nullptr) {
    return 0;
  }
  jmethodID mid = env->GetStaticMethodID(cls, "footerRowCount", "(Ljava/lang/String;)J");
  if (mid == nullptr) {
    return 0;
  }
  jstring jp = env->NewStringUTF(path.c_str());
  jlong n = env->CallStaticLongMethod(cls, mid, jp);
  env->DeleteLocalRef(jp);
  env->DeleteLocalRef(cls);
  return static_cast<long>(n);
}

VegamTable vegam_load_table(
    JNIEnv* env,
    const std::string& path,
    const std::vector<std::string>& cols,
    const std::vector<std::pair<std::string, std::string>>& parts) {
  VegamTable t;
  jclass cls = env->FindClass("org/apache/spark/sql/vegam/exec/HadoopBytes");
  if (cls == nullptr) {
    return t;
  }
  jmethodID mid = env->GetStaticMethodID(
      cls, "load",
      "(Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;)"
      "Lorg/apache/spark/sql/vegam/exec/NativeTable;");
  if (mid == nullptr) {
    return t;
  }
  jstring jp = env->NewStringUTF(path.c_str());
  jclass str_cls = env->FindClass("java/lang/String");
  jobjectArray jcols = env->NewObjectArray(static_cast<jsize>(cols.size()), str_cls, nullptr);
  for (size_t i = 0; i < cols.size(); i++) {
    env->SetObjectArrayElement(jcols, static_cast<jsize>(i),
                               env->NewStringUTF(cols[i].c_str()));
  }
  jobjectArray jpk = env->NewObjectArray(static_cast<jsize>(parts.size()), str_cls, nullptr);
  jobjectArray jpv = env->NewObjectArray(static_cast<jsize>(parts.size()), str_cls, nullptr);
  for (size_t i = 0; i < parts.size(); i++) {
    env->SetObjectArrayElement(jpk, static_cast<jsize>(i),
                               env->NewStringUTF(parts[i].first.c_str()));
    env->SetObjectArrayElement(jpv, static_cast<jsize>(i),
                               env->NewStringUTF(parts[i].second.c_str()));
  }
  jobject obj = env->CallStaticObjectMethod(cls, mid, jp, jcols, jpk, jpv);
  if (obj == nullptr) {
    return t;
  }
  jclass tcls = env->GetObjectClass(obj);
  jint nr = env->GetIntField(obj, env->GetFieldID(tcls, "numRows", "I"));
  jint nc = env->GetIntField(obj, env->GetFieldID(tcls, "numCols", "I"));
  auto names_arr = reinterpret_cast<jobjectArray>(
      env->GetObjectField(obj, env->GetFieldID(tcls, "names", "[Ljava/lang/String;")));
  auto values = reinterpret_cast<jdoubleArray>(
      env->GetObjectField(obj, env->GetFieldID(tcls, "values", "[D")));
  auto nulls = reinterpret_cast<jbooleanArray>(
      env->GetObjectField(obj, env->GetFieldID(tcls, "nulls", "[Z")));
  auto texts = reinterpret_cast<jobjectArray>(
      env->GetObjectField(obj, env->GetFieldID(tcls, "texts", "[Ljava/lang/String;")));
  t.num_rows = nr;
  t.cols.resize(nc);
  t.names.resize(nc);
  for (int c = 0; c < nc; c++) {
    auto js = reinterpret_cast<jstring>(env->GetObjectArrayElement(names_arr, c));
    t.names[c] = jstring_to_std(env, js);
    t.cols[c].values.assign(nr, 0);
    t.cols[c].nulls.assign(nr, 0);
    t.cols[c].texts.assign(nr, "");
  }
  if (nr > 0 && nc > 0 && values != nullptr) {
    std::vector<double> v(static_cast<size_t>(nr) * nc);
    std::vector<jboolean> nuls(static_cast<size_t>(nr) * nc);
    env->GetDoubleArrayRegion(values, 0, static_cast<jsize>(v.size()), v.data());
    env->GetBooleanArrayRegion(nulls, 0, static_cast<jsize>(nuls.size()), nuls.data());
    for (int r = 0; r < nr; r++) {
      for (int c = 0; c < nc; c++) {
        size_t i = static_cast<size_t>(r) * nc + c;
        t.cols[c].values[r] = v[i];
        t.cols[c].nulls[r] = nuls[i] ? 1 : 0;
        if (texts != nullptr) {
          auto ts = reinterpret_cast<jstring>(
              env->GetObjectArrayElement(texts, static_cast<jsize>(i)));
          if (ts != nullptr) {
            t.cols[c].texts[r] = jstring_to_std(env, ts);
            t.cols[c].text = true;
          }
        }
      }
    }
  }
  return t;
}

VegamTaskState* vegam_create_task(JNIEnv* env, const uint8_t* bytes, int n, int threads) {
  auto* task = new VegamTaskState();
  VegamDecoded plan;
  if (!vegam_decode_plan(bytes, n, &plan)) {
    return task;
  }
#ifdef VEGAM_HAS_VELOX
  if (plan.kind == 2 && plan.filters.empty() && plan.builds.empty() &&
      !plan.has_window && plan.groups.size() == 1 && plan.aggs.size() == 1) {
    std::vector<std::string> files;
    for (const auto& f : plan.probe.files) {
      files.push_back(f.path);
    }
    if (vegam_velox_scan_hash_agg(env, files, plan.groups[0], plan.aggs[0].col,
                                  &task->page)) {
      return task;
    }
    fprintf(stderr, "vegam: velox scan+hashagg missed, fused pipeline fallback\n");
  }
#endif
  int nthreads = threads > 0 ? threads : 1;
  fprintf(stderr, "vegam: fused-stage kind=%d threads=%d morsel=%d\n",
          plan.kind, nthreads, vegam::kMorselRows);
  task->page = vegam_run_decoded(env, plan, nthreads);
  return task;
}

int vegam_next_page(VegamTaskState* task, double* values, uint8_t* nulls, int* meta,
                    int cap) {
  if (task == nullptr || task->done) {
    return -1;
  }
  task->done = true;
  int rows = task->page.num_rows;
  int cols = static_cast<int>(task->page.cols.size());
  if (rows * cols > cap) {
    rows = cols == 0 ? 0 : cap / cols;
  }
  meta[0] = cols;
  for (int c = 0; c < cols && c < 3; c++) {
    meta[1 + c] = task->page.cols[c].scale;
  }
  for (int r = 0; r < rows; r++) {
    for (int c = 0; c < cols; c++) {
      int i = r * cols + c;
      values[i] = task->page.cols[c].values[r];
      nulls[i] = task->page.cols[c].nulls[r] ? 1 : 0;
    }
  }
  return rows;
}

void vegam_close_task(VegamTaskState* task) {
  delete task;
}
