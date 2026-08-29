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

#include <cstdio>
#include <cstdlib>
#include <cstring>

static void fail(const char *msg) {
  std::fprintf(stderr, "FAIL: %s\n", msg);
  std::exit(1);
}

int main() {
  int64_t ids[8] = {1, 2, 3, 4, 5, 6, 7, 8};
  int64_t vals[8] = {10, 20, 30, 40, 50, 60, 70, 80};
  NsCol cols[2];
  cols[0].type = NS_I64;
  cols[0].n_rows = 8;
  cols[0].data = ids;
  cols[1].type = NS_I64;
  cols[1].n_rows = 8;
  cols[1].data = vals;
  NsBatch in;
  in.n_cols = 2;
  in.n_rows = 8;
  in.cols = cols;

  NsBatch out{};
  if (ns_execute("(filter (gt c0 4i64) (scan 0))", &in, 1, &out) != 0) fail("filter");
  if (out.n_rows != 4) fail("filter row count");
  ns_batch_free(&out);

  if (ns_execute("(project (list c0 (add c0 c1)) (scan 0))", &in, 1, &out) != 0) fail("project");
  if (out.n_rows != 8 || out.n_cols != 2) fail("project shape");
  const int64_t *p1 = static_cast<const int64_t *>(out.cols[1].data);
  if (p1[0] != 11) fail("project add");
  ns_batch_free(&out);

  if (ns_execute("(hashagg (list c0) (list (sum c1)) (project (list c0 c1) (scan 0)))", &in, 1, &out) != 0) {
    fail("agg");
  }
  if (out.n_rows != 8) fail("agg groups");
  ns_batch_free(&out);

  int64_t rids[3] = {1, 3, 9};
  int64_t rvals[3] = {100, 300, 900};
  NsCol rcols[2];
  rcols[0].type = NS_I64;
  rcols[0].n_rows = 3;
  rcols[0].data = rids;
  rcols[1].type = NS_I64;
  rcols[1].n_rows = 3;
  rcols[1].data = rvals;
  NsBatch ins[2];
  ins[0] = in;
  ins[1].n_cols = 2;
  ins[1].n_rows = 3;
  ins[1].cols = rcols;
  if (ns_execute("(hashjoin c0 c0 (scan 0) (scan 1))", ins, 2, &out) != 0) fail("join");
  if (out.n_rows != 2) fail("join rows");
  ns_batch_free(&out);

  if (ns_execute("(filter (gt c0 5i64) (range 0 10 1))", nullptr, 0, &out) != 0) fail("range");
  if (out.n_rows != 4) fail("range filter"); /* 6,7,8,9 */
  ns_batch_free(&out);

  std::printf("nativesql_test: ok\n");
  return 0;
}
