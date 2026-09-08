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

#include <cmath>
#include <cstring>
#include <sstream>
#include <unordered_map>

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

long footer_rows(JNIEnv* env, const std::string& path) {
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

VegamTable load_table(JNIEnv* env, const std::string& path,
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
          auto ts = reinterpret_cast<jstring>(env->GetObjectArrayElement(texts, static_cast<jsize>(i)));
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

int32_t read_i32(const uint8_t*& p, const uint8_t* end) {
  if (p + 4 > end) {
    return 0;
  }
  int32_t v = (static_cast<int32_t>(p[0]) << 24) | (static_cast<int32_t>(p[1]) << 16) |
      (static_cast<int32_t>(p[2]) << 8) | static_cast<int32_t>(p[3]);
  p += 4;
  return v;
}

int64_t read_i64(const uint8_t*& p, const uint8_t* end) {
  if (p + 8 > end) {
    return 0;
  }
  int64_t v = (static_cast<int64_t>(p[0]) << 56) | (static_cast<int64_t>(p[1]) << 48) |
      (static_cast<int64_t>(p[2]) << 40) | (static_cast<int64_t>(p[3]) << 32) |
      (static_cast<int64_t>(p[4]) << 24) | (static_cast<int64_t>(p[5]) << 16) |
      (static_cast<int64_t>(p[6]) << 8) | static_cast<int64_t>(p[7]);
  p += 8;
  return v;
}

std::string read_str(const uint8_t*& p, const uint8_t* end) {
  int32_t n = read_i32(p, end);
  if (n < 0 || p + n > end) {
    return "";
  }
  std::string s(reinterpret_cast<const char*>(p), static_cast<size_t>(n));
  p += n;
  return s;
}

std::vector<std::string> read_strs(const uint8_t*& p, const uint8_t* end) {
  int32_t n = read_i32(p, end);
  std::vector<std::string> o;
  o.reserve(n);
  for (int i = 0; i < n; i++) {
    o.push_back(read_str(p, end));
  }
  return o;
}

VegamTable count_star(JNIEnv* env, const std::vector<std::string>& files) {
  double total = 0;
  for (const auto& f : files) {
    total += static_cast<double>(footer_rows(env, f));
  }
  VegamTable t;
  t.num_rows = 1;
  t.names = {"count"};
  t.cols.resize(1);
  t.cols[0].values = {total};
  t.cols[0].nulls = {0};
  t.cols[0].texts = {""};
  return t;
}

void hash_agg_sum(const VegamTable& in, int gcol, int scol, VegamTable* out) {
  std::unordered_map<long, double> sums;
  std::unordered_map<long, char> seen;
  if (gcol < 0 || scol < 0) {
    return;
  }
  for (int r = 0; r < in.num_rows; r++) {
    if (in.cols[gcol].nulls[r]) {
      continue;
    }
    long k = static_cast<long>(in.cols[gcol].values[r]);
    seen[k] = 1;
    if (!in.cols[scol].nulls[r]) {
      sums[k] += in.cols[scol].values[r];
    }
  }
  out->num_rows = static_cast<int>(seen.size());
  out->names = {in.names[gcol], in.names[scol]};
  out->cols.resize(2);
  out->cols[0].values.resize(out->num_rows);
  out->cols[0].nulls.resize(out->num_rows, 0);
  out->cols[1].values.resize(out->num_rows);
  out->cols[1].nulls.resize(out->num_rows, 0);
  int i = 0;
  for (const auto& kv : seen) {
    out->cols[0].values[i] = static_cast<double>(kv.first);
    auto it = sums.find(kv.first);
    if (it == sums.end()) {
      out->cols[1].nulls[i] = 1;
      out->cols[1].values[i] = NAN;
    } else {
      out->cols[1].values[i] = it->second;
    }
    i++;
  }
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

VegamTaskState* vegam_create_task(JNIEnv* env, const uint8_t* bytes, int n) {
  auto* task = new VegamTaskState();
  const uint8_t* p = bytes;
  const uint8_t* end = bytes + n;
  int version = read_i32(p, end);
  (void)version;
  int kind = read_i32(p, end);
  if (kind == 1) {
    task->page = count_star(env, read_strs(p, end));
  } else if (kind == 2) {
    auto files = read_strs(p, end);
    auto group = read_str(p, end);
    auto sumc = read_str(p, end);
#ifdef VEGAM_HAS_VELOX
    if (!vegam_velox_scan_hash_agg(env, files, group, sumc, &task->page)) {
      fprintf(stderr, "vegam: native HashAgg requires Velox scan+hashagg\n");
    }
#else
    VegamTable acc;
    bool first = true;
    for (const auto& f : files) {
      VegamTable part = load_table(env, f, {group, sumc}, {});
      if (first) {
        acc = std::move(part);
        first = false;
      } else {
        int g = acc.col_index(group);
        int s = acc.col_index(sumc);
        int pg = part.col_index(group);
        int ps = part.col_index(sumc);
        if (g >= 0 && s >= 0 && pg >= 0 && ps >= 0) {
          for (int r = 0; r < part.num_rows; r++) {
            acc.cols[g].values.push_back(part.cols[pg].values[r]);
            acc.cols[g].nulls.push_back(part.cols[pg].nulls[r]);
            acc.cols[s].values.push_back(part.cols[ps].values[r]);
            acc.cols[s].nulls.push_back(part.cols[ps].nulls[r]);
            acc.num_rows++;
          }
        }
      }
    }
    VegamTable out;
    hash_agg_sum(acc, acc.col_index(group), acc.col_index(sumc), &out);
    task->page = out;
#endif
  } else {
    task->page.num_rows = 0;
  }
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
