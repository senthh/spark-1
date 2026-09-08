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

#include "vegam_pipeline.h"
#include "vegam_scheduler.h"

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <sstream>
#include <unordered_map>

namespace {

constexpr int kFilterGt = 1;
constexpr int kFilterGte = 2;
constexpr int kFilterLt = 3;
constexpr int kFilterLte = 4;
constexpr int kFilterEq = 5;
constexpr int kFilterNe = 6;

constexpr int kAggSum = 1;
constexpr int kAggCount = 2;
constexpr int kAggCountStar = 3;
constexpr int kAggMin = 4;
constexpr int kAggMax = 5;
constexpr int kAggAvg = 6;

constexpr int kJoinInner = 1;
constexpr int kJoinLeft = 2;
constexpr int kJoinSemi = 3;
constexpr int kJoinAnti = 4;

constexpr int kWinRow = 1;
constexpr int kWinRank = 2;
constexpr int kWinDense = 3;
constexpr int kWinSum = 4;

constexpr int kExpandCol = 1;
constexpr int kExpandNull = 2;
constexpr int kExpandLong = 3;
constexpr int kExpandDouble = 4;
constexpr int kExpandStr = 5;

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

double read_f64(const uint8_t*& p, const uint8_t* end) {
  if (p + 8 > end) {
    return NAN;
  }
  uint64_t u = (static_cast<uint64_t>(p[0]) << 56) | (static_cast<uint64_t>(p[1]) << 48) |
      (static_cast<uint64_t>(p[2]) << 40) | (static_cast<uint64_t>(p[3]) << 32) |
      (static_cast<uint64_t>(p[4]) << 24) | (static_cast<uint64_t>(p[5]) << 16) |
      (static_cast<uint64_t>(p[6]) << 8) | static_cast<uint64_t>(p[7]);
  p += 8;
  double d;
  std::memcpy(&d, &u, 8);
  return d;
}

bool read_bool(const uint8_t*& p, const uint8_t* end) {
  if (p >= end) {
    return false;
  }
  return *p++;
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

std::vector<VegamFilter> read_filters(const uint8_t*& p, const uint8_t* end) {
  int32_t n = read_i32(p, end);
  std::vector<VegamFilter> o;
  o.reserve(n);
  for (int i = 0; i < n; i++) {
    VegamFilter f;
    f.col = read_str(p, end);
    f.value = read_i64(p, end);
    f.op = read_i32(p, end);
    f.str = read_str(p, end);
    f.dvalue = read_f64(p, end);
    f.is_string = !f.str.empty();
    o.push_back(f);
  }
  return o;
}

std::vector<VegamAgg> read_aggs(const uint8_t*& p, const uint8_t* end) {
  int32_t n = read_i32(p, end);
  std::vector<VegamAgg> o;
  o.reserve(n);
  for (int i = 0; i < n; i++) {
    VegamAgg a;
    a.kind = read_i32(p, end);
    a.col = read_str(p, end);
    a.scale = read_i32(p, end);
    read_str(p, end);
    o.push_back(a);
  }
  return o;
}

std::vector<VegamFileRef> read_refs(const uint8_t*& p, const uint8_t* end) {
  int32_t n = read_i32(p, end);
  std::vector<VegamFileRef> o;
  o.reserve(n);
  for (int i = 0; i < n; i++) {
    VegamFileRef r;
    r.path = read_str(p, end);
    int32_t pn = read_i32(p, end);
    for (int j = 0; j < pn; j++) {
      auto k = read_str(p, end);
      auto v = read_str(p, end);
      r.parts.emplace_back(k, v);
    }
    o.push_back(r);
  }
  return o;
}

VegamScan read_scan(const uint8_t*& p, const uint8_t* end) {
  VegamScan s;
  s.files = read_refs(p, end);
  s.columns = read_strs(p, end);
  return s;
}

double cell_num(const VegamTable& t, int col, int row) {
  if (col < 0 || row < 0 || row >= t.num_rows) {
    return NAN;
  }
  if (t.cols[col].nulls[row]) {
    return NAN;
  }
  return t.cols[col].values[row];
}

bool keep_row(const VegamTable& t, int row, const std::vector<VegamFilter>& fs) {
  static const std::string empty;
  for (const auto& f : fs) {
    int c = t.col_index(f.col);
    if (c < 0 || t.cols[c].nulls[row]) {
      return false;
    }
    if (f.is_string) {
      const std::string& s = t.cols[c].text ? t.cols[c].texts[row] : empty;
      if (f.op == kFilterEq && s != f.str) {
        return false;
      }
      if (f.op == kFilterNe && s == f.str) {
        return false;
      }
    } else {
      double v = t.cols[c].values[row];
      double tgt = std::isnan(f.dvalue) ? static_cast<double>(f.value) : f.dvalue;
      bool ok = false;
      switch (f.op) {
        case kFilterGt: ok = v > tgt; break;
        case kFilterGte: ok = v >= tgt; break;
        case kFilterLt: ok = v < tgt; break;
        case kFilterLte: ok = v <= tgt; break;
        case kFilterEq: ok = v == tgt; break;
        case kFilterNe: ok = v != tgt; break;
        default: ok = false;
      }
      if (!ok) {
        return false;
      }
    }
  }
  return true;
}

VegamTable filter_table(const VegamTable& in, const std::vector<VegamFilter>& fs, int threads) {
  if (fs.empty() || in.num_rows == 0) {
    return in;
  }
  std::vector<int> keep;
  keep.reserve(static_cast<size_t>(in.num_rows));
  std::mutex mu;
  vegam::run_morsels(in.num_rows, threads, [&](vegam::Morsel m) {
    std::vector<int> local;
    local.reserve(static_cast<size_t>(m.len));
    for (int r = m.start; r < m.start + m.len; r++) {
      if (keep_row(in, r, fs)) {
        local.push_back(r);
      }
    }
    std::lock_guard<std::mutex> g(mu);
    keep.insert(keep.end(), local.begin(), local.end());
  });
  std::sort(keep.begin(), keep.end());
  VegamTable out;
  out.names = in.names;
  out.cols.resize(in.cols.size());
  out.num_rows = static_cast<int>(keep.size());
  for (size_t c = 0; c < in.cols.size(); c++) {
    out.cols[c].text = in.cols[c].text;
    out.cols[c].scale = in.cols[c].scale;
    out.cols[c].values.resize(keep.size());
    out.cols[c].nulls.resize(keep.size());
    out.cols[c].texts.resize(keep.size());
    for (size_t i = 0; i < keep.size(); i++) {
      int r = keep[i];
      out.cols[c].values[i] = in.cols[c].values[r];
      out.cols[c].nulls[i] = in.cols[c].nulls[r];
      out.cols[c].texts[i] = in.cols[c].texts[r];
    }
  }
  return out;
}

std::string row_key(const VegamTable& t, const std::vector<int>& idx, int row) {
  std::ostringstream os;
  for (size_t i = 0; i < idx.size(); i++) {
    if (i) {
      os << '\x01';
    }
    int c = idx[i];
    if (c < 0 || t.cols[c].nulls[row]) {
      return std::string();
    }
    if (t.cols[c].text && !t.cols[c].texts[row].empty()) {
      os << t.cols[c].texts[row];
    } else {
      os << t.cols[c].values[row];
    }
  }
  return os.str();
}

bool monotonic_on(const VegamTable& t, const std::vector<int>& keys) {
  if (t.num_rows < 2 || keys.empty()) {
    return false;
  }
  int lim = std::min(t.num_rows, 256);
  std::string prev = row_key(t, keys, 0);
  for (int r = 1; r < lim; r++) {
    std::string k = row_key(t, keys, r);
    if (k < prev) {
      return false;
    }
    prev.swap(k);
  }
  return true;
}

void append_table(VegamTable* acc, const VegamTable& part) {
  if (acc->names.empty()) {
    *acc = part;
    return;
  }
  if (part.num_rows == 0) {
    return;
  }
  int start = acc->num_rows;
  acc->num_rows += part.num_rows;
  for (size_t c = 0; c < acc->cols.size() && c < part.cols.size(); c++) {
    acc->cols[c].values.resize(acc->num_rows);
    acc->cols[c].nulls.resize(acc->num_rows);
    acc->cols[c].texts.resize(acc->num_rows);
    for (int r = 0; r < part.num_rows; r++) {
      acc->cols[c].values[start + r] = part.cols[c].values[r];
      acc->cols[c].nulls[start + r] = part.cols[c].nulls[r];
      acc->cols[c].texts[start + r] = part.cols[c].texts[r];
    }
  }
}

VegamTable hash_join(const VegamTable& probe, const VegamTable& build, const VegamBuild& spec,
                     int threads) {
  std::vector<int> pk;
  std::vector<int> bk;
  for (const auto& n : spec.probe_keys) {
    pk.push_back(probe.col_index(n));
  }
  for (const auto& n : spec.build_keys) {
    bk.push_back(build.col_index(n));
  }
  std::unordered_map<std::string, std::vector<int>> idx;
  for (int r = 0; r < build.num_rows; r++) {
    std::string k = row_key(build, bk, r);
    if (!k.empty()) {
      idx[k].push_back(r);
    }
  }
  std::vector<int> extra;
  for (int c = 0; c < static_cast<int>(build.names.size()); c++) {
    bool skip = false;
    for (const auto& n : spec.build_keys) {
      if (build.names[c] == n) {
        skip = true;
      }
    }
    for (const auto& n : probe.names) {
      if (build.names[c] == n) {
        skip = true;
      }
    }
    if (!skip) {
      extra.push_back(c);
    }
  }
  const bool project_probe_only =
      spec.join_type == kJoinSemi || spec.join_type == kJoinAnti;
  VegamTable out;
  out.names = probe.names;
  out.cols.resize(probe.cols.size() + (project_probe_only ? 0 : extra.size()));
  if (!project_probe_only) {
    for (int c : extra) {
      out.names.push_back(build.names[c]);
      out.cols[out.names.size() - 1].text = build.cols[c].text;
    }
  }
  std::mutex mu;
  vegam::run_morsels(probe.num_rows, threads, [&](vegam::Morsel m) {
    VegamTable local;
    local.names = out.names;
    local.cols.resize(out.cols.size());
    for (size_t c = 0; c < out.cols.size(); c++) {
      local.cols[c].text = out.cols[c].text;
    }
    auto push_probe = [&](int pr, int br, bool has_b) {
      int at = local.num_rows++;
      for (size_t c = 0; c < probe.cols.size(); c++) {
        local.cols[c].values.push_back(probe.cols[c].values[pr]);
        local.cols[c].nulls.push_back(probe.cols[c].nulls[pr]);
        local.cols[c].texts.push_back(probe.cols[c].texts[pr]);
      }
      if (!project_probe_only) {
        for (size_t i = 0; i < extra.size(); i++) {
          size_t o = probe.cols.size() + i;
          if (!has_b) {
            local.cols[o].values.push_back(NAN);
            local.cols[o].nulls.push_back(1);
            local.cols[o].texts.push_back("");
          } else {
            int c = extra[i];
            local.cols[o].values.push_back(build.cols[c].values[br]);
            local.cols[o].nulls.push_back(build.cols[c].nulls[br]);
            local.cols[o].texts.push_back(build.cols[c].texts[br]);
          }
        }
      }
      (void)at;
    };
    for (int r = m.start; r < m.start + m.len; r++) {
      std::string k = row_key(probe, pk, r);
      auto it = k.empty() ? idx.end() : idx.find(k);
      bool hit = it != idx.end() && !it->second.empty();
      switch (spec.join_type) {
        case kJoinInner:
          if (hit) {
            for (int br : it->second) {
              push_probe(r, br, true);
            }
          }
          break;
        case kJoinLeft:
          if (!hit) {
            push_probe(r, -1, false);
          } else {
            for (int br : it->second) {
              push_probe(r, br, true);
            }
          }
          break;
        case kJoinSemi:
          if (hit) {
            push_probe(r, -1, false);
          }
          break;
        case kJoinAnti:
          if (!hit) {
            push_probe(r, -1, false);
          }
          break;
        default:
          break;
      }
    }
    std::lock_guard<std::mutex> g(mu);
    append_table(&out, local);
  });
  return out;
}

VegamTable merge_join(const VegamTable& probe, const VegamTable& build, const VegamBuild& spec) {
  std::vector<int> pk;
  std::vector<int> bk;
  for (const auto& n : spec.probe_keys) {
    pk.push_back(probe.col_index(n));
  }
  for (const auto& n : spec.build_keys) {
    bk.push_back(build.col_index(n));
  }
  std::vector<int> pord(probe.num_rows);
  std::vector<int> bord(build.num_rows);
  for (int i = 0; i < probe.num_rows; i++) {
    pord[i] = i;
  }
  for (int i = 0; i < build.num_rows; i++) {
    bord[i] = i;
  }
  std::stable_sort(pord.begin(), pord.end(), [&](int a, int b) {
    return row_key(probe, pk, a) < row_key(probe, pk, b);
  });
  std::stable_sort(bord.begin(), bord.end(), [&](int a, int b) {
    return row_key(build, bk, a) < row_key(build, bk, b);
  });
  VegamTable sp;
  VegamTable sb;
  sp.names = probe.names;
  sb.names = build.names;
  sp.cols.resize(probe.cols.size());
  sb.cols.resize(build.cols.size());
  sp.num_rows = probe.num_rows;
  sb.num_rows = build.num_rows;
  for (size_t c = 0; c < probe.cols.size(); c++) {
    sp.cols[c].text = probe.cols[c].text;
    sp.cols[c].values.resize(pord.size());
    sp.cols[c].nulls.resize(pord.size());
    sp.cols[c].texts.resize(pord.size());
    for (size_t i = 0; i < pord.size(); i++) {
      int r = pord[i];
      sp.cols[c].values[i] = probe.cols[c].values[r];
      sp.cols[c].nulls[i] = probe.cols[c].nulls[r];
      sp.cols[c].texts[i] = probe.cols[c].texts[r];
    }
  }
  for (size_t c = 0; c < build.cols.size(); c++) {
    sb.cols[c].text = build.cols[c].text;
    sb.cols[c].values.resize(bord.size());
    sb.cols[c].nulls.resize(bord.size());
    sb.cols[c].texts.resize(bord.size());
    for (size_t i = 0; i < bord.size(); i++) {
      int r = bord[i];
      sb.cols[c].values[i] = build.cols[c].values[r];
      sb.cols[c].nulls[i] = build.cols[c].nulls[r];
      sb.cols[c].texts[i] = build.cols[c].texts[r];
    }
  }
  return hash_join(sp, sb, spec, 1);
}

struct AggAcc {
  std::vector<double> sum;
  std::vector<double> minv;
  std::vector<double> maxv;
  std::vector<long> count;
  std::vector<char> has;
  std::vector<double> gvals;
  std::vector<char> gnull;
  std::vector<std::string> gtext;
};

void acc_add(AggAcc* a, const VegamTable& t, int row, const std::vector<int>& gidx,
             const std::vector<VegamAgg>& aggs) {
  if (a->sum.empty()) {
    a->sum.assign(aggs.size(), 0);
    a->minv.assign(aggs.size(), 0);
    a->maxv.assign(aggs.size(), 0);
    a->count.assign(aggs.size(), 0);
    a->has.assign(aggs.size(), 0);
    a->gvals.assign(gidx.size(), 0);
    a->gnull.assign(gidx.size(), 0);
    a->gtext.assign(gidx.size(), "");
    for (size_t i = 0; i < gidx.size(); i++) {
      int c = gidx[i];
      if (c >= 0) {
        a->gvals[i] = t.cols[c].values[row];
        a->gnull[i] = t.cols[c].nulls[row];
        a->gtext[i] = t.cols[c].texts[row];
      } else {
        a->gnull[i] = 1;
      }
    }
  }
  for (size_t i = 0; i < aggs.size(); i++) {
    const auto& ag = aggs[i];
    if (ag.kind == kAggCountStar) {
      a->count[i]++;
      a->has[i] = 1;
      continue;
    }
    int c = ag.col.empty() ? -1 : t.col_index(ag.col);
    if (ag.kind == kAggCount) {
      if (c >= 0 && !t.cols[c].nulls[row]) {
        a->count[i]++;
        a->has[i] = 1;
      }
      continue;
    }
    if (c < 0 || t.cols[c].nulls[row]) {
      continue;
    }
    double v = t.cols[c].values[row];
    if (!a->has[i]) {
      a->minv[i] = v;
      a->maxv[i] = v;
      a->has[i] = 1;
    } else {
      if (v < a->minv[i]) {
        a->minv[i] = v;
      }
      if (v > a->maxv[i]) {
        a->maxv[i] = v;
      }
    }
    a->sum[i] += v;
    a->count[i]++;
  }
}

void acc_merge(AggAcc* dst, const AggAcc& src) {
  if (dst->sum.empty()) {
    *dst = src;
    return;
  }
  for (size_t i = 0; i < src.sum.size(); i++) {
    dst->sum[i] += src.sum[i];
    dst->count[i] += src.count[i];
    if (src.has[i]) {
      if (!dst->has[i]) {
        dst->minv[i] = src.minv[i];
        dst->maxv[i] = src.maxv[i];
        dst->has[i] = 1;
      } else {
        if (src.minv[i] < dst->minv[i]) {
          dst->minv[i] = src.minv[i];
        }
        if (src.maxv[i] > dst->maxv[i]) {
          dst->maxv[i] = src.maxv[i];
        }
      }
    }
  }
}

double acc_result(const AggAcc& a, size_t i, int kind) {
  if (!a.has[i] && kind != kAggCount && kind != kAggCountStar) {
    return NAN;
  }
  switch (kind) {
    case kAggSum: return a.sum[i];
    case kAggCount:
    case kAggCountStar: return static_cast<double>(a.count[i]);
    case kAggMin: return a.minv[i];
    case kAggMax: return a.maxv[i];
    case kAggAvg: return a.count[i] == 0 ? NAN : a.sum[i] / static_cast<double>(a.count[i]);
    default: return NAN;
  }
}

VegamTable emit_agg(
    const std::unordered_map<std::string, AggAcc>& state,
    const VegamTable& src,
    const std::vector<int>& gidx,
    const std::vector<VegamAgg>& aggs) {
  VegamTable out;
  out.num_rows = static_cast<int>(state.size());
  out.names.reserve(gidx.size() + aggs.size());
  for (int c : gidx) {
    out.names.push_back(c >= 0 ? src.names[c] : "?");
  }
  for (const auto& a : aggs) {
    out.names.push_back(a.col.empty() ? "agg" : a.col);
  }
  out.cols.resize(out.names.size());
  for (auto& c : out.cols) {
    c.values.resize(out.num_rows);
    c.nulls.resize(out.num_rows);
    c.texts.resize(out.num_rows);
  }
  int r = 0;
  for (const auto& kv : state) {
    const AggAcc& a = kv.second;
    for (size_t i = 0; i < gidx.size(); i++) {
      out.cols[i].values[r] = a.gvals[i];
      out.cols[i].nulls[r] = a.gnull[i];
      out.cols[i].texts[r] = a.gtext[i];
    }
    for (size_t i = 0; i < aggs.size(); i++) {
      double v = acc_result(a, i, aggs[i].kind);
      size_t o = gidx.size() + i;
      out.cols[o].values[r] = v;
      out.cols[o].nulls[r] = std::isnan(v) ? 1 : 0;
      out.cols[o].scale = aggs[i].scale;
    }
    r++;
  }
  return out;
}

VegamTable hash_agg(const VegamTable& in, const std::vector<std::string>& groups,
                    const std::vector<VegamAgg>& aggs, int threads) {
  std::vector<int> gidx;
  for (const auto& g : groups) {
    gidx.push_back(in.col_index(g));
  }
  if (threads > 1 && in.num_rows > vegam::kMorselRows) {
    std::vector<std::unordered_map<std::string, AggAcc>> locals(static_cast<size_t>(threads));
    std::atomic<int> slot{0};
    vegam::run_morsels(in.num_rows, threads, [&](vegam::Morsel m) {
      int mine = slot.fetch_add(1) % threads;
      auto& st = locals[static_cast<size_t>(mine)];
      for (int r = m.start; r < m.start + m.len; r++) {
        std::string k = gidx.empty() ? std::string() : row_key(in, gidx, r);
        acc_add(&st[k], in, r, gidx, aggs);
      }
    });
    std::unordered_map<std::string, AggAcc> merged;
    for (auto& loc : locals) {
      for (auto& kv : loc) {
        acc_merge(&merged[kv.first], kv.second);
      }
    }
    return emit_agg(merged, in, gidx, aggs);
  }
  std::unordered_map<std::string, AggAcc> state;
  for (int r = 0; r < in.num_rows; r++) {
    std::string k = gidx.empty() ? std::string() : row_key(in, gidx, r);
    acc_add(&state[k], in, r, gidx, aggs);
  }
  return emit_agg(state, in, gidx, aggs);
}

VegamTable sort_agg(const VegamTable& in, const std::vector<std::string>& groups,
                    const std::vector<VegamAgg>& aggs) {
  std::vector<int> gidx;
  for (const auto& g : groups) {
    gidx.push_back(in.col_index(g));
  }
  std::vector<int> ord(in.num_rows);
  for (int i = 0; i < in.num_rows; i++) {
    ord[i] = i;
  }
  std::stable_sort(ord.begin(), ord.end(), [&](int a, int b) {
    return row_key(in, gidx, a) < row_key(in, gidx, b);
  });
  std::unordered_map<std::string, AggAcc> state;
  std::string prev;
  bool first = true;
  AggAcc cur;
  for (int r : ord) {
    std::string k = gidx.empty() ? std::string() : row_key(in, gidx, r);
    if (first || k != prev) {
      if (!first) {
        state[prev] = cur;
      }
      cur = AggAcc();
      prev = k;
      first = false;
    }
    acc_add(&cur, in, r, gidx, aggs);
  }
  if (!first) {
    state[prev] = cur;
  }
  fprintf(stderr, "vegam: kernel=sort-agg groups=%zu rows=%d\n", groups.size(), in.num_rows);
  return emit_agg(state, in, gidx, aggs);
}

VegamTable window_table(const VegamTable& in, const VegamWin& w) {
  std::vector<int> pidx;
  std::vector<int> oidx;
  std::vector<char> oasc;
  for (const auto& n : w.partition) {
    pidx.push_back(in.col_index(n));
  }
  for (const auto& o : w.order) {
    oidx.push_back(in.col_index(o.first));
    oasc.push_back(o.second ? 1 : 0);
  }
  std::vector<int> ord(in.num_rows);
  for (int i = 0; i < in.num_rows; i++) {
    ord[i] = i;
  }
  std::stable_sort(ord.begin(), ord.end(), [&](int a, int b) {
    std::string pa = row_key(in, pidx, a);
    std::string pb = row_key(in, pidx, b);
    if (pa != pb) {
      return pa < pb;
    }
    for (size_t i = 0; i < oidx.size(); i++) {
      double va = cell_num(in, oidx[i], a);
      double vb = cell_num(in, oidx[i], b);
      if (va < vb) {
        return oasc[i] != 0;
      }
      if (va > vb) {
        return oasc[i] == 0;
      }
    }
    return false;
  });
  VegamTable out = in;
  int extra = static_cast<int>(w.fns.size());
  out.cols.resize(in.cols.size() + extra);
  for (int i = 0; i < extra; i++) {
    out.names.push_back(w.fns[i].alias);
    out.cols[in.cols.size() + i].values.assign(in.num_rows, 0);
    out.cols[in.cols.size() + i].nulls.assign(in.num_rows, 0);
    out.cols[in.cols.size() + i].texts.assign(in.num_rows, "");
  }
  int i = 0;
  while (i < static_cast<int>(ord.size())) {
    std::string part = row_key(in, pidx, ord[i]);
    int j = i;
    while (j < static_cast<int>(ord.size()) && row_key(in, pidx, ord[j]) == part) {
      j++;
    }
    std::vector<double> sums(w.fns.size(), 0);
    for (size_t f = 0; f < w.fns.size(); f++) {
      if (w.fns[f].kind == kWinSum) {
        int c = in.col_index(w.fns[f].col);
        for (int k = i; k < j; k++) {
          double v = cell_num(in, c, ord[k]);
          if (!std::isnan(v)) {
            sums[f] += v;
          }
        }
      }
    }
    std::string prev;
    int rank = 0;
    int dense = 0;
    for (int k = i; k < j; k++) {
      std::string ok = row_key(in, oidx, ord[k]);
      if (ok != prev) {
        rank = k - i + 1;
        dense++;
        prev = ok;
      }
      int dst = ord[k];
      for (size_t f = 0; f < w.fns.size(); f++) {
        size_t o = in.cols.size() + f;
        double v = 0;
        switch (w.fns[f].kind) {
          case kWinRow: v = static_cast<double>(k - i + 1); break;
          case kWinRank: v = static_cast<double>(rank); break;
          case kWinDense: v = static_cast<double>(dense); break;
          case kWinSum: v = sums[f]; break;
          default: v = NAN;
        }
        out.cols[o].values[dst] = v;
        out.cols[o].nulls[dst] = std::isnan(v) ? 1 : 0;
      }
    }
    i = j;
  }
  fprintf(stderr, "vegam: kernel=window rows=%d\n", in.num_rows);
  return out;
}

VegamTable expand_table(const VegamTable& in, const VegamExpand& spec) {
  VegamTable out;
  out.names = spec.out_cols;
  out.cols.resize(spec.out_cols.size());
  for (const auto& proj : spec.projections) {
    for (int r = 0; r < in.num_rows; r++) {
      for (size_t c = 0; c < spec.out_cols.size(); c++) {
        double v = NAN;
        char n = 1;
        std::string t;
        bool text = false;
        if (c < proj.size()) {
          const auto& s = proj[c];
          if (s.kind == kExpandCol) {
            int i = in.col_index(s.col);
            if (i >= 0) {
              v = in.cols[i].values[r];
              n = in.cols[i].nulls[r];
              t = in.cols[i].texts[r];
              text = in.cols[i].text;
            }
          } else if (s.kind == kExpandLong) {
            v = static_cast<double>(s.lvalue);
            n = 0;
          } else if (s.kind == kExpandDouble) {
            v = s.dvalue;
            n = 0;
          } else if (s.kind == kExpandStr) {
            t = s.svalue;
            n = 0;
            text = true;
          }
        }
        out.cols[c].values.push_back(v);
        out.cols[c].nulls.push_back(n);
        out.cols[c].texts.push_back(t);
        if (text) {
          out.cols[c].text = true;
        }
      }
      out.num_rows++;
    }
  }
  fprintf(stderr, "vegam: kernel=expand rows=%d projs=%zu\n",
          out.num_rows, spec.projections.size());
  return out;
}

}  // namespace

bool vegam_decode_plan(const uint8_t* bytes, int n, VegamDecoded* out) {
  const uint8_t* p = bytes;
  const uint8_t* end = bytes + n;
  int version = read_i32(p, end);
  int kind = read_i32(p, end);
  out->kind = kind;
  if (kind == 1) {
    auto files = read_strs(p, end);
    for (const auto& f : files) {
      out->probe.files.push_back(VegamFileRef{f, {}});
    }
    return true;
  }
  if (kind == 2) {
    auto files = read_strs(p, end);
    out->group_col = read_str(p, end);
    out->sum_col = read_str(p, end);
    if (version >= 2) {
      out->filters = read_filters(p, end);
      read_i32(p, end);
      read_str(p, end);
      read_str(p, end);
      out->complete = read_bool(p, end);
      out->groups = read_strs(p, end);
      out->aggs = read_aggs(p, end);
      out->probe.files = read_refs(p, end);
    } else {
      for (const auto& f : files) {
        out->probe.files.push_back(VegamFileRef{f, {}});
      }
    }
    if (out->groups.empty() && !out->group_col.empty()) {
      out->groups = {out->group_col};
    }
    if (out->aggs.empty() && !out->sum_col.empty()) {
      out->aggs.push_back(VegamAgg{kAggSum, out->sum_col, 0});
    }
    if (out->probe.files.empty()) {
      for (const auto& f : files) {
        out->probe.files.push_back(VegamFileRef{f, {}});
      }
    }
    return true;
  }
  if (kind == 3) {
    out->probe = read_scan(p, end);
    int nb = read_i32(p, end);
    for (int i = 0; i < nb; i++) {
      VegamBuild b;
      b.scan = read_scan(p, end);
      b.probe_keys = read_strs(p, end);
      b.build_keys = read_strs(p, end);
      b.join_type = read_i32(p, end);
      b.filters = read_filters(p, end);
      out->builds.push_back(b);
    }
    out->filters = read_filters(p, end);
    out->groups = read_strs(p, end);
    read_strs(p, end);
    out->aggs = read_aggs(p, end);
    out->has_window = read_bool(p, end);
    if (out->has_window) {
      out->window.partition = read_strs(p, end);
      int on = read_i32(p, end);
      for (int i = 0; i < on; i++) {
        auto c = read_str(p, end);
        bool asc = read_bool(p, end);
        out->window.order.emplace_back(c, asc);
      }
      int fn = read_i32(p, end);
      for (int i = 0; i < fn; i++) {
        VegamWinFn f;
        f.kind = read_i32(p, end);
        f.col = read_str(p, end);
        f.alias = read_str(p, end);
        out->window.fns.push_back(f);
      }
    }
    out->complete = read_bool(p, end);
    if (version >= 3 && p < end) {
      out->has_expand = read_bool(p, end);
      if (out->has_expand) {
        out->expand.out_cols = read_strs(p, end);
        int np = read_i32(p, end);
        for (int i = 0; i < np; i++) {
          int w = read_i32(p, end);
          std::vector<VegamExpandSlot> row;
          row.reserve(w);
          for (int j = 0; j < w; j++) {
            VegamExpandSlot s;
            s.kind = read_i32(p, end);
            s.col = read_str(p, end);
            s.lvalue = read_i64(p, end);
            s.dvalue = read_f64(p, end);
            s.svalue = read_str(p, end);
            row.push_back(s);
          }
          out->expand.projections.push_back(std::move(row));
        }
      }
    }
    return true;
  }
  return false;
}

VegamTable vegam_load_scan(JNIEnv* env, const VegamScan& scan,
                           const std::vector<std::string>& extra,
                           const std::vector<VegamFilter>& filters) {
  std::vector<std::string> cols = scan.columns;
  for (const auto& e : extra) {
    if (!e.empty()) {
      cols.push_back(e);
    }
  }
  for (const auto& f : filters) {
    cols.push_back(f.col);
  }
  VegamTable acc;
  for (const auto& ref : scan.files) {
    VegamTable part = vegam_load_table(env, ref.path, cols, ref.parts);
    append_table(&acc, part);
  }
  if (scan.files.empty() && !scan.columns.empty()) {
    return acc;
  }
  return acc;
}

VegamTable vegam_run_decoded(JNIEnv* env, const VegamDecoded& plan, int threads) {
  int nthreads = threads > 0 ? threads : 1;
  if (plan.kind == 1) {
    double total = 0;
    for (const auto& f : plan.probe.files) {
      total += static_cast<double>(vegam_footer_rows(env, f.path));
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
  std::vector<std::string> want = plan.groups;
  for (const auto& a : plan.aggs) {
    want.push_back(a.col);
  }
  for (const auto& f : plan.filters) {
    want.push_back(f.col);
  }
  for (const auto& b : plan.builds) {
    want.insert(want.end(), b.probe_keys.begin(), b.probe_keys.end());
  }
  if (plan.has_window) {
    want.insert(want.end(), plan.window.partition.begin(), plan.window.partition.end());
    for (const auto& o : plan.window.order) {
      want.push_back(o.first);
    }
    for (const auto& f : plan.window.fns) {
      want.push_back(f.col);
    }
  }
  if (plan.has_expand) {
    for (const auto& proj : plan.expand.projections) {
      for (const auto& s : proj) {
        if (!s.col.empty()) {
          want.push_back(s.col);
        }
      }
    }
  }
  want.insert(want.end(), plan.probe.columns.begin(), plan.probe.columns.end());
  VegamScan probe = plan.probe;
  if (probe.columns.empty()) {
    probe.columns = want;
  }
  VegamTable table = vegam_load_scan(env, probe, want, plan.filters);
  table = filter_table(table, plan.filters, nthreads);
  for (const auto& b : plan.builds) {
    VegamTable build = vegam_load_scan(env, b.scan, b.build_keys, b.filters);
    build = filter_table(build, b.filters, nthreads);
    std::vector<int> pk;
    std::vector<int> bk;
    for (const auto& n : b.probe_keys) {
      pk.push_back(table.col_index(n));
    }
    for (const auto& n : b.build_keys) {
      bk.push_back(build.col_index(n));
    }
    bool merge = monotonic_on(table, pk) && monotonic_on(build, bk) &&
        table.num_rows > vegam::kMorselRows && build.num_rows > vegam::kMorselRows;
    if (merge) {
      fprintf(stderr, "vegam: kernel=merge-join probe=%d build=%d\n",
              table.num_rows, build.num_rows);
      table = merge_join(table, build, b);
    } else {
      fprintf(stderr, "vegam: kernel=hash-join probe=%d build=%d type=%d\n",
              table.num_rows, build.num_rows, b.join_type);
      table = hash_join(table, build, b, nthreads);
    }
  }
  if (plan.has_expand) {
    table = expand_table(table, plan.expand);
  }
  if (plan.has_window) {
    table = window_table(table, plan.window);
  }
  if (!plan.aggs.empty() || !plan.groups.empty()) {
    bool sorted = false;
    std::vector<int> gidx;
    for (const auto& g : plan.groups) {
      gidx.push_back(table.col_index(g));
    }
    sorted = monotonic_on(table, gidx);
    if (sorted) {
      return sort_agg(table, plan.groups, plan.aggs);
    }
    fprintf(stderr, "vegam: kernel=hash-agg morsels threads=%d rows=%d\n",
            nthreads, table.num_rows);
    return hash_agg(table, plan.groups, plan.aggs, nthreads);
  }
  return table;
}
