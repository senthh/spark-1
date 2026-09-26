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

#ifndef VEGAM_ENGINE_H
#define VEGAM_ENGINE_H

#include <jni.h>
#include <cstdint>
#include <string>
#include <vector>

struct VegamCol {
  std::vector<double> values;
  std::vector<char> nulls;
  std::vector<std::string> texts;
  int scale = 0;
  bool text = false;
};

struct VegamTable {
  std::vector<std::string> names;
  std::vector<VegamCol> cols;
  int num_rows = 0;

  int col_index(const std::string& name) const;
};

struct VegamTaskState {
  VegamTable page;
  int row_off = 0;
  bool done = false;
  // Velox run streaming Arrow batches; null when the fused pipeline ran.
  void* velox = nullptr;
};

VegamTable vegam_load_table(
    JNIEnv* env,
    const std::string& path,
    const std::vector<std::string>& cols,
    const std::vector<std::pair<std::string, std::string>>& parts);
long vegam_footer_rows(JNIEnv* env, const std::string& path);

VegamTaskState* vegam_create_task(JNIEnv* env, const uint8_t* bytes, int n,
                                  int threads);
int vegam_next_page(VegamTaskState* task, double* values, uint8_t* nulls,
                    int* meta, int cap);
void vegam_close_task(VegamTaskState* task);

#ifdef VEGAM_HAS_VELOX
struct VegamDecoded;

// Plans and starts a Velox run for the stage, or returns null when the stage
// shape is not covered (the caller then runs the fused C++ pipeline).
void* vegam_velox_start(JNIEnv* env, const VegamDecoded& plan, int threads);

// Exports the next batch into ArrowArray / ArrowSchema structs. Returns the
// row count, or -1 at end. Throws std::exception on failure.
int vegam_velox_next(void* run, void* arrow_array, void* arrow_schema);

void vegam_velox_close(void* run);
#endif

#endif
