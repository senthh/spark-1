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
#include "sort.h"

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

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

  if (ns_execute("(filter (or (eq c0 1i64) (eq c0 2i64)) (scan 0))", &in, 1, &out) != 0) {
    fail("or");
  }
  if (out.n_rows != 2) fail("or row count");
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

  /* --- column permutation sort --- */

  /* already-sorted ints: trySort should return identity */
  {
    int64_t sorted[8] = {1, 2, 3, 4, 5, 6, 7, 8};
    NsCol k{};
    k.type = NS_I64;
    k.n_rows = 8;
    k.data = sorted;
    uint32_t perm[8];
    if (ns_sort_permutation(&k, 1, 8, perm) != 0) fail("trySort call");
    for (int i = 0; i < 8; ++i) {
      if (perm[i] != static_cast<uint32_t>(i)) fail("trySort identity");
    }
    if (ns_execute("(sort (list c0) (scan 0))", &in, 1, &out) != 0) fail("sort already");
    if (out.n_rows != 8) fail("sort already rows");
    const int64_t *got = static_cast<const int64_t *>(out.cols[0].data);
    for (int i = 0; i < 8; ++i) {
      if (got[i] != ids[i]) fail("sort already values");
    }
    ns_batch_free(&out);
  }

  /* one adjacent inversion: trySort swaps the pair */
  {
    int64_t almost[5] = {1, 2, 4, 3, 5};
    NsCol k{};
    k.type = NS_I64;
    k.n_rows = 5;
    k.data = almost;
    uint32_t perm[5];
    if (ns_sort_permutation(&k, 1, 5, perm) != 0) fail("one inv call");
    int64_t ordered[5];
    for (int i = 0; i < 5; ++i) ordered[i] = almost[perm[i]];
    for (int i = 1; i < 5; ++i) {
      if (ordered[i - 1] > ordered[i]) fail("one inv order");
    }
  }

  /* small n < 256: comparison sort path */
  {
    int64_t small_k[6] = {9, 1, 4, 1, 7, 3};
    int64_t small_p[6] = {90, 10, 40, 11, 70, 30};
    NsCol sc[2];
    sc[0].type = NS_I64;
    sc[0].n_rows = 6;
    sc[0].data = small_k;
    sc[1].type = NS_I64;
    sc[1].n_rows = 6;
    sc[1].data = small_p;
    NsBatch sb{2, 6, sc};
    if (ns_execute("(sort (list c0) (scan 0))", &sb, 1, &out) != 0) fail("small sort");
    if (out.n_rows != 6) fail("small sort rows");
    const int64_t *gk = static_cast<const int64_t *>(out.cols[0].data);
    const int64_t *gp = static_cast<const int64_t *>(out.cols[1].data);
    int64_t expect_k[6] = {1, 1, 3, 4, 7, 9};
    for (int i = 0; i < 6; ++i) {
      if (gk[i] != expect_k[i]) fail("small sort keys");
    }
    if (gp[0] != 10 && gp[0] != 11) fail("small sort payload");
    if (gp[0] + gp[1] != 21) fail("small sort both ones");
    ns_batch_free(&out);
  }

  /* random int64 vs std::sort of indices (hits LSD radix, n >= 256) */
  {
    const int n = 512;
    std::vector<int64_t> rk(static_cast<size_t>(n));
    uint64_t rng = 1;
    for (int i = 0; i < n; ++i) {
      rng = rng * 6364136223846793005ULL + 1;
      rk[static_cast<size_t>(i)] = static_cast<int64_t>(rng);
    }
    /* mix in negatives so sign-bit mapping is tested */
    rk[0] = INT64_MIN;
    rk[1] = -1;
    rk[2] = 0;
    rk[3] = INT64_MAX;
    NsCol k{};
    k.type = NS_I64;
    k.n_rows = n;
    k.data = rk.data();
    std::vector<uint32_t> perm(static_cast<size_t>(n));
    if (ns_sort_permutation(&k, 1, n, perm.data()) != 0) fail("rand i64 call");
    std::vector<uint32_t> expect(static_cast<size_t>(n));
    for (int i = 0; i < n; ++i) expect[static_cast<size_t>(i)] = static_cast<uint32_t>(i);
    std::sort(expect.begin(), expect.end(), [&](uint32_t a, uint32_t b) {
      return rk[a] < rk[b];
    });
    for (int i = 0; i < n; ++i) {
      if (rk[perm[static_cast<size_t>(i)]] != rk[expect[static_cast<size_t>(i)]]) {
        fail("rand i64 vs std::sort");
      }
    }
    std::vector<uint8_t> seen(static_cast<size_t>(n), 0);
    for (int i = 0; i < n; ++i) {
      const uint32_t idx = perm[static_cast<size_t>(i)];
      if (idx >= static_cast<uint32_t>(n) || seen[idx]) fail("rand i64 perm");
      seen[idx] = 1;
    }
    /* already-sorted large range: trySort, skip radix */
    std::vector<int64_t> seq(static_cast<size_t>(n));
    for (int i = 0; i < n; ++i) seq[static_cast<size_t>(i)] = i;
    k.data = seq.data();
    if (ns_sort_permutation(&k, 1, n, perm.data()) != 0) fail("sorted radix trySort");
    for (int i = 0; i < n; ++i) {
      if (perm[static_cast<size_t>(i)] != static_cast<uint32_t>(i)) fail("sorted large identity");
    }
  }

  /* I32 radix (4-byte keys, n >= 256) */
  {
    const int n = 300;
    std::vector<int32_t> ik(static_cast<size_t>(n));
    for (int i = 0; i < n; ++i) ik[static_cast<size_t>(i)] = 150 - i;
    ik[10] = -100000;
    NsCol k{};
    k.type = NS_I32;
    k.n_rows = n;
    k.data = ik.data();
    std::vector<uint32_t> perm(static_cast<size_t>(n));
    if (ns_sort_permutation(&k, 1, n, perm.data()) != 0) fail("i32 radix");
    for (int i = 1; i < n; ++i) {
      if (ik[perm[static_cast<size_t>(i - 1)]] > ik[perm[static_cast<size_t>(i)]]) {
        fail("i32 radix order");
      }
    }
  }

  /* multi-column: sort by c0 then c1 */
  {
    int64_t m0[4] = {2, 1, 2, 1};
    int64_t m1[4] = {9, 8, 7, 6};
    int64_t pay[4] = {20, 10, 21, 11};
    NsCol mc[3];
    mc[0].type = NS_I64;
    mc[0].n_rows = 4;
    mc[0].data = m0;
    mc[1].type = NS_I64;
    mc[1].n_rows = 4;
    mc[1].data = m1;
    mc[2].type = NS_I64;
    mc[2].n_rows = 4;
    mc[2].data = pay;
    NsBatch mb{3, 4, mc};
    if (ns_execute("(sort (list c0 c1) (scan 0))", &mb, 1, &out) != 0) fail("multi sort");
    if (out.n_rows != 4 || out.n_cols != 3) fail("multi sort shape");
    const int64_t *g0 = static_cast<const int64_t *>(out.cols[0].data);
    const int64_t *g1 = static_cast<const int64_t *>(out.cols[1].data);
    const int64_t *g2 = static_cast<const int64_t *>(out.cols[2].data);
    /* (1,6,11), (1,8,10), (2,7,21), (2,9,20) */
    if (g0[0] != 1 || g1[0] != 6 || g2[0] != 11) fail("multi 0");
    if (g0[1] != 1 || g1[1] != 8 || g2[1] != 10) fail("multi 1");
    if (g0[2] != 2 || g1[2] != 7 || g2[2] != 21) fail("multi 2");
    if (g0[3] != 2 || g1[3] != 9 || g2[3] != 20) fail("multi 3");
    ns_batch_free(&out);
  }

  /* float keys use comparison sort */
  {
    double fk[4] = {3.5, -1.0, 2.25, 0.0};
    NsCol fc{};
    fc.type = NS_F64;
    fc.n_rows = 4;
    fc.data = fk;
    NsBatch fb{1, 4, &fc};
    if (ns_execute("(sort (list c0) (scan 0))", &fb, 1, &out) != 0) fail("f64 sort");
    const double *gf = static_cast<const double *>(out.cols[0].data);
    if (gf[0] != -1.0 || gf[1] != 0.0 || gf[2] != 2.25 || gf[3] != 3.5) fail("f64 order");
    ns_batch_free(&out);
  }

  /* sort over range IR */
  if (ns_execute("(sort (list c0) (filter (gt c0 5i64) (range 0 10 1)))", nullptr, 0, &out) != 0) {
    fail("sort range");
  }
  if (out.n_rows != 4) fail("sort range rows");
  {
    const int64_t *gr = static_cast<const int64_t *>(out.cols[0].data);
    if (gr[0] != 6 || gr[3] != 9) fail("sort range values");
  }
  ns_batch_free(&out);

  std::printf("nativesql_test: ok\n");
  return 0;
}
