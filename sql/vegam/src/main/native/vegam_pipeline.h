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

#ifndef VEGAM_PIPELINE_H
#define VEGAM_PIPELINE_H

#include "vegam_engine.h"

#include <string>
#include <utility>
#include <vector>

struct VegamFilter {
  std::string col;
  int64_t value = 0;
  int op = 0;
  std::string str;
  double dvalue = 0.0 / 0.0;
  bool is_string = false;
};

struct VegamFileRef {
  std::string path;
  std::vector<std::pair<std::string, std::string>> parts;
};

struct VegamScan {
  std::vector<VegamFileRef> files;
  std::vector<std::string> columns;
};

struct VegamAgg {
  int kind = 0;
  std::string col;
  int scale = 0;
};

struct VegamBuild {
  VegamScan scan;
  std::vector<std::string> probe_keys;
  std::vector<std::string> build_keys;
  int join_type = 1;
  std::vector<VegamFilter> filters;
};

struct VegamWinFn {
  int kind = 0;
  std::string col;
  std::string alias;
};

struct VegamWin {
  std::vector<std::string> partition;
  std::vector<std::pair<std::string, bool>> order;
  std::vector<VegamWinFn> fns;
};

struct VegamExpandSlot {
  int kind = 0;
  std::string col;
  int64_t lvalue = 0;
  double dvalue = 0;
  std::string svalue;
};

struct VegamExpand {
  std::vector<std::string> out_cols;
  std::vector<std::vector<VegamExpandSlot>> projections;
};

struct VegamDecoded {
  int kind = 0;
  VegamScan probe;
  std::vector<VegamBuild> builds;
  std::vector<VegamFilter> filters;
  std::vector<std::string> groups;
  std::vector<VegamAgg> aggs;
  bool has_window = false;
  VegamWin window;
  bool complete = false;
  bool has_expand = false;
  VegamExpand expand;
  std::string group_col;
  std::string sum_col;
};

bool vegam_decode_plan(const uint8_t* bytes, int n, VegamDecoded* out);

VegamTable vegam_load_scan(JNIEnv* env, const VegamScan& scan,
                           const std::vector<std::string>& extra,
                           const std::vector<VegamFilter>& filters);

VegamTable vegam_run_decoded(JNIEnv* env, const VegamDecoded& plan, int threads);

#endif
