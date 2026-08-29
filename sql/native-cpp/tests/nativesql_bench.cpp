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

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <vector>

int main() {
  const int n = 1 << 20;
  std::vector<int64_t> k(n), v(n);
  for (int i = 0; i < n; ++i) {
    k[i] = i % 1024;
    v[i] = i;
  }
  NsCol cols[2];
  cols[0].type = NS_I64;
  cols[0].n_rows = n;
  cols[0].data = k.data();
  cols[1].type = NS_I64;
  cols[1].n_rows = n;
  cols[1].data = v.data();
  NsBatch in{2, n, cols};

  const char *ir =
      "(hashagg (list c0) (list (sum c1)) (filter (gt c1 16i64) (project (list c0 c1) (scan 0))))";

  auto t0 = std::chrono::steady_clock::now();
  NsBatch out{};
  if (ns_execute(ir, &in, 1, &out) != 0) {
    std::fprintf(stderr, "bench execute failed\n");
    return 1;
  }
  auto t1 = std::chrono::steady_clock::now();
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
  std::printf("nativesql_bench: %d rows -> %d groups in %lld ms\n", n, out.n_rows,
              static_cast<long long>(ms));
  ns_batch_free(&out);
  return 0;
}
