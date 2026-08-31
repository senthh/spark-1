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
#include "sort.h"

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/writer.h>

#include <algorithm>
#include <cstdint>
#include <memory>
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
  NsBatch empty{};
  empty.n_cols = 0;
  empty.n_rows = 0;
  empty.cols = nullptr;
  if (ns_execute("(project (list c0) (scan 0))", &empty, 1, &out) != 0) {
    fail("empty project");
  }
  if (out.n_rows != 0) fail("empty project rows");
  ns_batch_free(&out);

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

  /* two quantity buckets: 1-20 and 21-40; qty 45 is dropped */
  {
    int64_t qty[4] = {1, 5, 25, 45};
    int64_t disc[4] = {10, 20, 30, 40};
    int64_t prof[4] = {1, 2, 3, 4};
    NsCol sc[3];
    sc[0].type = NS_I64;
    sc[0].n_rows = 4;
    sc[0].data = qty;
    sc[1].type = NS_I64;
    sc[1].n_rows = 4;
    sc[1].data = disc;
    sc[2].type = NS_I64;
    sc[2].n_rows = 4;
    sc[2].data = prof;
    NsBatch sb;
    sb.n_cols = 3;
    sb.n_rows = 4;
    sb.cols = sc;
    if (ns_execute(
            "(segagg c0 (list 1i64 20i64 21i64 40i64) "
            "(list (count) (sum c1) (count c1) (sum c2) (count c2)) (scan 0))",
            &sb, 1, &out) != 0) {
      fail("segagg");
    }
    if (out.n_rows != 1 || out.n_cols != 10) fail("segagg shape");
    const int64_t *c0 = static_cast<const int64_t *>(out.cols[0].data);
    const int64_t *s1 = static_cast<const int64_t *>(out.cols[1].data);
    const int64_t *c5 = static_cast<const int64_t *>(out.cols[5].data);
    const int64_t *s6 = static_cast<const int64_t *>(out.cols[6].data);
    if (c0[0] != 2 || s1[0] != 30) fail("segagg bucket0");
    if (c5[0] != 1 || s6[0] != 30) fail("segagg bucket1");
    ns_batch_free(&out);
  }

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
  if (ns_execute("(hashjoinidx c0 c0 (scan 0) (scan 1))", ins, 2, &out) != 0) fail("joinidx");
  if (out.n_rows != 2) fail("joinidx rows");
  ns_batch_free(&out);

  /* key-only i32 batches (Spark hashjoinidx path) */
  {
    int32_t lk[3] = {1, 2, 3};
    int32_t rk[2] = {2, 3};
    NsCol lc{};
    lc.type = NS_I32;
    lc.n_rows = 3;
    lc.data = lk;
    NsCol rc{};
    rc.type = NS_I32;
    rc.n_rows = 2;
    rc.data = rk;
    NsBatch lb{1, 3, &lc};
    NsBatch rb{1, 2, &rc};
    NsBatch pair[2] = {lb, rb};
    if (ns_execute("(hashjoinidx c0 c0 (scan 0) (scan 1))", pair, 2, &out) != 0) {
      fail("joinidx i32");
    }
    if (out.n_rows != 2) fail("joinidx i32 rows");
    ns_batch_free(&out);
    NsBatch empty{0, 0, nullptr};
    NsBatch zpair[2] = {lb, empty};
    if (ns_execute("(hashjoinidx c0 c0 (scan 0) (scan 1))", zpair, 2, &out) != 0) {
      fail("joinidx empty");
    }
    if (out.n_rows != 0) fail("joinidx empty rows");
    ns_batch_free(&out);
  }

  if (ns_execute("(hashsemi c0 c0 (scan 0) (scan 1))", ins, 2, &out) != 0) fail("semi");
  if (out.n_rows != 2) fail("semi rows");
  ns_batch_free(&out);

  if (ns_execute("(union (scan 0) (scan 1))", ins, 2, &out) != 0) fail("union");
  if (out.n_rows != 11) fail("union rows"); /* 8 + 3 */
  ns_batch_free(&out);

  if (ns_execute("(project (list (if (gt c0 4i64) c0 0i64)) (scan 0))", &in, 1, &out) != 0) {
    fail("if");
  }
  if (out.n_rows != 8) fail("if rows");
  ns_batch_free(&out);

  if (ns_execute(
          "(hashagg (list c0) (list (sum c1)) (hashjoin c0 c0 (scan 0) (scan 1)))",
          ins, 2, &out) != 0) {
    fail("joinagg");
  }
  if (out.n_rows != 2) fail("joinagg rows");
  ns_batch_free(&out);
  if (ns_execute(
          "(hashagg (list c0) (list (sum c1)) (filter (gt c1 0i64) (hashjoin c0 c0 (scan 0) (scan 1))))",
          ins, 2, &out) != 0) {
    fail("joinagg filter");
  }
  if (out.n_rows != 2) fail("joinagg filter rows");
  ns_batch_free(&out);

  {
    std::shared_ptr<arrow::Array> a0, a1;
    arrow::Int32Builder b0;
    arrow::Int64Builder b1;
    if (!b0.AppendValues({1, 2, 3, 4}).ok() || !b0.Finish(&a0).ok()) fail("pq build0");
    if (!b1.AppendValues({10, 20, 30, 40}).ok() || !b1.Finish(&a1).ok()) fail("pq build1");
    auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                 arrow::field("v", arrow::int64())});
    auto table = arrow::Table::Make(schema, {a0, a1});
    const char *pqpath = "/tmp/nativesql_scan_test.parquet";
    auto outf = arrow::io::FileOutputStream::Open(pqpath);
    if (!outf.ok()) fail("pq open");
    if (!parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *outf, 1024).ok()) {
      fail("pq write");
    }
    if (!(*outf)->Close().ok()) fail("pq close");
    NsFileSplit sp{};
    sp.path = pqpath;
    sp.start = 0;
    sp.length = 0;
    const char *nms[2] = {"id", "v"};
    int32_t tps[2] = {NS_I32, NS_I64};
    NsFileScan sc{};
    sc.splits = &sp;
    sc.n_splits = 1;
    sc.col_names = nms;
    sc.col_types = tps;
    sc.n_cols = 2;
    if (ns_execute_scan("(filter (gt c0 2i64) (scan 0))", nullptr, &sc, 1, &out) != 0) {
      fail("pq scan");
    }
    if (out.n_rows != 2) fail("pq scan rows");
    ns_batch_free(&out);
    /* Missing / extra names must not abort (partition cols, case fold). */
    const char *nms2[3] = {"ID", "missing_part", "v"};
    int32_t tps2[3] = {NS_I32, NS_I64, NS_I64};
    sc.col_names = nms2;
    sc.col_types = tps2;
    sc.n_cols = 3;
    if (ns_execute_scan("(scan 0)", nullptr, &sc, 1, &out) != 0) fail("pq miss");
    if (out.n_rows != 4) fail("pq miss rows");
    const int64_t *miss = static_cast<const int64_t *>(out.cols[1].data);
    for (int i = 0; i < 4; ++i) {
      if (miss[i] != 0) fail("pq miss zeros");
    }
    ns_batch_free(&out);
    /* Split range that hits no row group must return 0 rows, not abort. */
    sp.start = 1LL << 40;
    sp.length = 1;
    sc.col_names = nms;
    sc.col_types = tps;
    sc.n_cols = 2;
    if (ns_execute_scan("(scan 0)", nullptr, &sc, 1, &out) != 0) fail("pq empty split");
    if (out.n_rows != 0) fail("pq empty split rows");
    ns_batch_free(&out);
    sp.start = 0;
    sp.length = 0;
  }

  /* Two probe parquet files as two splits: join/agg must not concat first. */
  {
    auto write_kv = [](const char *path, const std::vector<int32_t> &k,
                       const std::vector<int64_t> &v) {
      arrow::Int32Builder b0;
      arrow::Int64Builder b1;
      std::shared_ptr<arrow::Array> a0, a1;
      if (!b0.AppendValues(k).ok() || !b0.Finish(&a0).ok()) fail("fat kv0");
      if (!b1.AppendValues(v).ok() || !b1.Finish(&a1).ok()) fail("fat kv1");
      auto schema = arrow::schema(
          {arrow::field("id", arrow::int32()), arrow::field("v", arrow::int64())});
      auto table = arrow::Table::Make(schema, {a0, a1});
      auto outf = arrow::io::FileOutputStream::Open(path);
      if (!outf.ok()) fail("fat open");
      if (!parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *outf, 1024)
               .ok()) {
        fail("fat write");
      }
      if (!(*outf)->Close().ok()) fail("fat close");
    };
    auto write_k = [](const char *path, const std::vector<int32_t> &k) {
      arrow::Int32Builder b0;
      std::shared_ptr<arrow::Array> a0;
      if (!b0.AppendValues(k).ok() || !b0.Finish(&a0).ok()) fail("fat dim0");
      auto schema = arrow::schema({arrow::field("id", arrow::int32())});
      auto table = arrow::Table::Make(schema, {a0});
      auto outf = arrow::io::FileOutputStream::Open(path);
      if (!outf.ok()) fail("fat dim open");
      if (!parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *outf, 1024)
               .ok()) {
        fail("fat dim write");
      }
      if (!(*outf)->Close().ok()) fail("fat dim close");
    };
    const char *p0 = "/tmp/nativesql_fat_probe0.parquet";
    const char *p1 = "/tmp/nativesql_fat_probe1.parquet";
    const char *pd = "/tmp/nativesql_fat_dim.parquet";
    write_kv(p0, {1, 2}, {10, 20});
    write_kv(p1, {1, 3}, {30, 40});
    write_k(pd, {1, 3});
    NsFileSplit psp[2]{};
    psp[0].path = p0;
    psp[0].start = 0;
    psp[0].length = 0;
    psp[1].path = p1;
    psp[1].start = 0;
    psp[1].length = 0;
    NsFileSplit dsp{};
    dsp.path = pd;
    dsp.start = 0;
    dsp.length = 0;
    const char *pn[2] = {"id", "v"};
    int32_t pt[2] = {NS_I32, NS_I64};
    const char *dn[1] = {"id"};
    int32_t dt[1] = {NS_I32};
    NsFileScan scans[2]{};
    scans[0].splits = psp;
    scans[0].n_splits = 2;
    scans[0].col_names = pn;
    scans[0].col_types = pt;
    scans[0].n_cols = 2;
    scans[1].splits = &dsp;
    scans[1].n_splits = 1;
    scans[1].col_names = dn;
    scans[1].col_types = dt;
    scans[1].n_cols = 1;
    if (ns_execute_scan("(project (list c1) (hashjoin c0 c0 (scan 0) (scan 1)))", nullptr,
                        scans, 2, &out) != 0) {
      fail("fat join");
    }
    if (out.n_rows != 3) fail("fat join rows");
    const int64_t *pv = static_cast<const int64_t *>(out.cols[0].data);
    if (pv[0] != 10 || pv[1] != 30 || pv[2] != 40) fail("fat join vals");
    ns_batch_free(&out);
    if (ns_execute_scan(
            "(hashagg (list c0) (list c0 (sum c1)) (hashjoin c0 c0 (scan 0) (scan 1)))",
            nullptr, scans, 2, &out) != 0) {
      fail("fat hashagg");
    }
    if (out.n_rows != 2) fail("fat hashagg rows");
    const int64_t *gk = static_cast<const int64_t *>(out.cols[0].data);
    const int64_t *gs = static_cast<const int64_t *>(out.cols[1].data);
    int64_t s1 = 0, s3 = 0;
    for (int i = 0; i < out.n_rows; ++i) {
      if (gk[i] == 1) s1 = gs[i];
      else if (gk[i] == 3) s3 = gs[i];
      else fail("fat hashagg key");
    }
    if (s1 != 40 || s3 != 40) fail("fat hashagg sum");
    ns_batch_free(&out);
  }

  /* Empty name must not read that leaf (column prune). */
  {
    arrow::Int32Builder b0;
    arrow::StringBuilder bs;
    arrow::Int64Builder b2;
    std::shared_ptr<arrow::Array> a0, as, a2;
    if (!b0.AppendValues({1, 2, 3}).ok() || !b0.Finish(&a0).ok()) fail("prune0");
    if (!bs.Append("aaa").ok() || !bs.Append("bbb").ok() || !bs.Append("ccc").ok() ||
        !bs.Finish(&as).ok()) {
      fail("prune s");
    }
    if (!b2.AppendValues({10, 20, 30}).ok() || !b2.Finish(&a2).ok()) fail("prune2");
    auto schema = arrow::schema({arrow::field("id", arrow::int32()),
                                 arrow::field("payload", arrow::utf8()),
                                 arrow::field("v", arrow::int64())});
    auto table = arrow::Table::Make(schema, {a0, as, a2});
    const char *pqpath = "/tmp/nativesql_prune.parquet";
    auto outf = arrow::io::FileOutputStream::Open(pqpath);
    if (!outf.ok()) fail("prune open");
    if (!parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *outf, 1024)
             .ok()) {
      fail("prune write");
    }
    if (!(*outf)->Close().ok()) fail("prune close");
    NsFileSplit sp{};
    sp.path = pqpath;
    sp.start = 0;
    sp.length = 0;
    const char *nms[3] = {"id", "", "v"};
    int32_t tps[3] = {NS_I32, NS_I64, NS_I64};
    NsFileScan sc{};
    sc.splits = &sp;
    sc.n_splits = 1;
    sc.col_names = nms;
    sc.col_types = tps;
    sc.n_cols = 3;
    if (ns_execute_scan("(project (list c0 c2) (scan 0))", nullptr, &sc, 1, &out) != 0) {
      fail("prune scan");
    }
    if (out.n_rows != 3 || out.n_cols != 2) fail("prune shape");
    const int64_t *ids = static_cast<const int64_t *>(out.cols[0].data);
    const int64_t *vs = static_cast<const int64_t *>(out.cols[1].data);
    if (ids[0] != 1 || ids[2] != 3 || vs[1] != 20) fail("prune vals");
    ns_batch_free(&out);
    if (ns_execute_scan("(scan 0)", nullptr, &sc, 1, &out) != 0) fail("prune raw");
    const int64_t *mid = static_cast<const int64_t *>(out.cols[1].data);
    for (int i = 0; i < out.n_rows; ++i) {
      if (mid[i] != 0) fail("prune skipped col");
    }
    ns_batch_free(&out);
  }

  /* STRING / BINARY (invalid UTF-8) / DECIMAL / TIMESTAMP must not abort. */
  {
    std::shared_ptr<arrow::Array> as, ab, ad, at;
    arrow::StringBuilder sb;
    arrow::BinaryBuilder bb;
    arrow::Decimal128Builder db(arrow::decimal128(10, 2));
    arrow::TimestampBuilder tb(arrow::timestamp(arrow::TimeUnit::MICRO),
                               arrow::default_memory_pool());
    if (!sb.Append("ok").ok() || !sb.Append("xy").ok() || !sb.Finish(&as).ok()) {
      fail("pq str");
    }
    const uint8_t bad[] = {0xff, 0xfe, 0x80, 0x00};
    if (!bb.Append(bad, 4).ok() || !bb.Append("z", 1).ok() || !bb.Finish(&ab).ok()) {
      fail("pq bin");
    }
    if (!db.Append(arrow::Decimal128(12345)).ok() ||
        !db.Append(arrow::Decimal128(7)).ok() || !db.Finish(&ad).ok()) {
      fail("pq dec");
    }
    if (!tb.Append(1000).ok() || !tb.Append(2000).ok() || !tb.Finish(&at).ok()) {
      fail("pq ts");
    }
    auto schema = arrow::schema(
        {arrow::field("s", arrow::utf8()), arrow::field("b", arrow::binary()),
         arrow::field("d", arrow::decimal128(10, 2)),
         arrow::field("t", arrow::timestamp(arrow::TimeUnit::MICRO))});
    auto table = arrow::Table::Make(schema, {as, ab, ad, at});
    const char *pqpath = "/tmp/nativesql_scan_types.parquet";
    auto outf = arrow::io::FileOutputStream::Open(pqpath);
    if (!outf.ok()) fail("pq types open");
    if (!parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *outf, 1024).ok()) {
      fail("pq types write");
    }
    if (!(*outf)->Close().ok()) fail("pq types close");
    NsFileSplit sp{};
    sp.path = pqpath;
    const char *nms[4] = {"s", "b", "d", "t"};
    int32_t tps[4] = {NS_I64, NS_I64, NS_I64, NS_I64};
    NsFileScan sc{};
    sc.splits = &sp;
    sc.n_splits = 1;
    sc.col_names = nms;
    sc.col_types = tps;
    sc.n_cols = 4;
    ns_strdict_clear();
    if (ns_execute_scan("(scan 0)", nullptr, &sc, 1, &out) != 0) fail("pq types");
    if (out.n_rows != 2) fail("pq types rows");
    const int64_t *dec = static_cast<const int64_t *>(out.cols[2].data);
    const int64_t *ts = static_cast<const int64_t *>(out.cols[3].data);
    if (dec[0] != 12345 || dec[1] != 7) fail("pq dec val");
    if (ts[0] != 1000 || ts[1] != 2000) fail("pq ts val");
    if (ns_strdict_size() < 2) fail("pq strdict");
    ns_batch_free(&out);
    ns_strdict_clear();
  }

  /* Row-group stats skip: year=2000 must drop the 1999 groups. */
  {
    arrow::Int32Builder by;
    arrow::Int64Builder bv;
    for (int i = 0; i < 2000; ++i) {
      if (!by.Append(1999).ok() || !bv.Append(i).ok()) fail("skip append");
    }
    for (int i = 0; i < 50; ++i) {
      if (!by.Append(2000).ok() || !bv.Append(10000 + i).ok()) fail("skip append2");
    }
    std::shared_ptr<arrow::Array> ay, av;
    if (!by.Finish(&ay).ok() || !bv.Finish(&av).ok()) fail("skip finish");
    auto schema = arrow::schema({arrow::field("year", arrow::int32()),
                                 arrow::field("v", arrow::int64())});
    auto table = arrow::Table::Make(schema, {ay, av});
    const char *pqpath = "/tmp/nativesql_skip.parquet";
    auto outf = arrow::io::FileOutputStream::Open(pqpath);
    if (!outf.ok()) fail("skip open");
    if (!parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), *outf, 1000)
             .ok()) {
      fail("skip write");
    }
    if (!(*outf)->Close().ok()) fail("skip close");
    NsFileSplit sp{};
    sp.path = pqpath;
    const char *nms[2] = {"year", "v"};
    int32_t tps[2] = {NS_I32, NS_I64};
    NsFileScan sc{};
    sc.splits = &sp;
    sc.n_splits = 1;
    sc.col_names = nms;
    sc.col_types = tps;
    sc.n_cols = 2;
    if (ns_execute_scan("(filter (eq c0 2000i32) (scan 0))", nullptr, &sc, 1, &out) !=
        0) {
      fail("skip scan");
    }
    if (out.n_rows != 50) fail("skip rows");
    ns_batch_free(&out);
    NsColPred pred{};
    pred.col = 0;
    pred.op = 1;
    pred.value = 1999;
    sc.preds = &pred;
    sc.n_preds = 1;
    if (ns_execute_scan("(scan 0)", nullptr, &sc, 1, &out) != 0) fail("skip pred");
    if (out.n_rows != 2000) fail("skip pred rows");
    ns_batch_free(&out);
    pred.value = 2001;
    if (ns_execute_scan("(scan 0)", nullptr, &sc, 1, &out) != 0) fail("skip none");
    if (out.n_rows != 0) fail("skip none rows");
    ns_batch_free(&out);
  }

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
