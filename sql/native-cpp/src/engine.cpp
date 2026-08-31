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

#include <cctype>
#include <cmath>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <stdexcept>

static thread_local const NsFileScan *g_file_scans = nullptr;

namespace {

enum class ValKind { I64, F64, BOOL, COL, CALL };

struct Val {
  ValKind kind = ValKind::I64;
  int64_t i = 0;
  double d = 0;
  bool b = false;
  std::string name;
  std::vector<Val> args;
};

struct Tok {
  const char *p;
  explicit Tok(const char *s) : p(s) {}
  void skip() {
    while (*p == ' ' || *p == '\n' || *p == '\t') ++p;
  }
  bool eat(char c) {
    skip();
    if (*p == c) {
      ++p;
      return true;
    }
    return false;
  }
  std::string ident() {
    skip();
    const char *s = p;
    while (*p && *p != ' ' && *p != '(' && *p != ')' && *p != '\n' && *p != '\t') ++p;
    return std::string(s, p);
  }
};

Val parse_val(Tok &t);

Val parse_list(Tok &t) {
  if (!t.eat('(')) throw std::runtime_error("expected '('");
  Val v;
  v.kind = ValKind::CALL;
  v.name = t.ident();
  t.skip();
  while (*t.p && *t.p != ')') {
    v.args.push_back(parse_val(t));
    t.skip();
  }
  if (!t.eat(')')) throw std::runtime_error("expected ')'");
  return v;
}

Val parse_val(Tok &t) {
  t.skip();
  if (*t.p == '(') return parse_list(t);
  std::string tok = t.ident();
  Val v;
  if (tok == "true") {
    v.kind = ValKind::BOOL;
    v.b = true;
    return v;
  }
  if (tok == "false") {
    v.kind = ValKind::BOOL;
    v.b = false;
    return v;
  }
  if (!tok.empty() && tok[0] == 'c' && tok.size() > 1 && std::isdigit(static_cast<unsigned char>(tok[1]))) {
    v.kind = ValKind::COL;
    v.i = std::stoll(tok.substr(1));
    return v;
  }
  if (tok.size() > 3 && tok.compare(tok.size() - 3, 3, "i32") == 0) {
    v.kind = ValKind::I64;
    v.i = std::stoll(tok.substr(0, tok.size() - 3));
    return v;
  }
  if (tok.size() > 3 && tok.compare(tok.size() - 3, 3, "i64") == 0) {
    v.kind = ValKind::I64;
    v.i = std::stoll(tok.substr(0, tok.size() - 3));
    return v;
  }
  if (tok.size() > 3 && tok.compare(tok.size() - 3, 3, "f64") == 0) {
    v.kind = ValKind::F64;
    v.d = std::stod(tok.substr(0, tok.size() - 3));
    return v;
  }
  /* bare integer / float (scan index, range bounds) */
  try {
    if (tok.find('.') != std::string::npos) {
      v.kind = ValKind::F64;
      v.d = std::stod(tok);
      return v;
    }
    v.kind = ValKind::I64;
    v.i = std::stoll(tok);
    return v;
  } catch (...) {
    throw std::runtime_error("bad token: " + tok);
  }
}

struct Batch {
  int n_rows = 0;
  std::vector<NsType> types;
  std::vector<std::vector<int64_t>> i64;
  std::vector<std::vector<double>> f64;
  std::vector<std::vector<uint8_t>> b;

  int n_cols() const { return static_cast<int>(types.size()); }

  void add_col(NsType t, int n) {
    types.push_back(t);
    i64.emplace_back();
    f64.emplace_back();
    b.emplace_back();
    n_rows = n;
    const int c = n_cols() - 1;
    if (t == NS_F64) f64[c].assign(n, 0);
    else if (t == NS_BOOL) b[c].assign(n, 0);
    else i64[c].assign(n, 0);
  }

  int64_t get_i(int col, int row) const {
    if (col < 0 || col >= n_cols() || row < 0 || row >= n_rows) return 0;
    if (types[col] == NS_F64) {
      if (row >= static_cast<int>(f64[col].size())) return 0;
      return static_cast<int64_t>(f64[col][row]);
    }
    if (types[col] == NS_BOOL) {
      if (row >= static_cast<int>(b[col].size())) return 0;
      return b[col][row] ? 1 : 0;
    }
    if (row >= static_cast<int>(i64[col].size())) return 0;
    return i64[col][row];
  }
  double get_f(int col, int row) const {
    if (col < 0 || col >= n_cols() || row < 0 || row >= n_rows) return 0;
    if (types[col] == NS_F64) {
      if (row >= static_cast<int>(f64[col].size())) return 0;
      return f64[col][row];
    }
    if (types[col] == NS_BOOL) {
      if (row >= static_cast<int>(b[col].size())) return 0;
      return b[col][row] ? 1.0 : 0.0;
    }
    if (row >= static_cast<int>(i64[col].size())) return 0;
    return static_cast<double>(i64[col][row]);
  }
  bool get_b(int col, int row) const { return get_i(col, row) != 0; }
};

Batch from_c(const NsBatch &in) {
  Batch b;
  b.n_rows = in.n_rows < 0 ? 0 : in.n_rows;
  if (in.cols == nullptr || in.n_cols <= 0) {
    return b;
  }
  for (int c = 0; c < in.n_cols; ++c) {
    const NsCol &col = in.cols[c];
    b.add_col(col.type, in.n_rows);
    if (col.data == nullptr) {
      continue;
    }
    const int n = in.n_rows < col.n_rows ? in.n_rows : col.n_rows;
    if (n <= 0) continue;
    if (col.type == NS_I32) {
      const int32_t *p = static_cast<const int32_t *>(col.data);
      for (int r = 0; r < n; ++r) b.i64[c][r] = p[r];
    } else if (col.type == NS_I64) {
      std::memcpy(b.i64[c].data(), col.data, sizeof(int64_t) * static_cast<size_t>(n));
    } else if (col.type == NS_F64) {
      std::memcpy(b.f64[c].data(), col.data, sizeof(double) * static_cast<size_t>(n));
    } else {
      std::memcpy(b.b[c].data(), col.data, static_cast<size_t>(n));
    }
  }
  return b;
}

struct EvalNum {
  bool is_f = false;
  int64_t i = 0;
  double d = 0;
  double as_f() const { return is_f ? d : static_cast<double>(i); }
  int64_t as_i() const { return is_f ? static_cast<int64_t>(d) : i; }
};

bool eval_pred(const Val &v, const Batch &b, int row);

EvalNum eval_num(const Val &v, const Batch &b, int row) {
  if (v.kind == ValKind::I64) return {false, v.i, 0};
  if (v.kind == ValKind::F64) return {true, 0, v.d};
  if (v.kind == ValKind::BOOL) return {false, v.b ? 1 : 0, 0};
  if (v.kind == ValKind::COL) {
    const int c = static_cast<int>(v.i);
    if (c < 0 || c >= b.n_cols() || row < 0 || row >= b.n_rows) {
      return {false, 0, 0};
    }
    if (b.types[c] == NS_F64) return {true, 0, b.get_f(c, row)};
    return {false, b.get_i(c, row), 0};
  }
  if (v.kind == ValKind::CALL) {
    const auto &n = v.name;
    if (n == "add" || n == "sub" || n == "mul" || n == "div") {
      EvalNum l = eval_num(v.args[0], b, row);
      EvalNum r = eval_num(v.args[1], b, row);
      const bool f = l.is_f || r.is_f;
      if (n == "add") {
        if (f) return {true, 0, l.as_f() + r.as_f()};
        return {false, l.i + r.i, 0};
      }
      if (n == "sub") {
        if (f) return {true, 0, l.as_f() - r.as_f()};
        return {false, l.i - r.i, 0};
      }
      if (n == "mul") {
        if (f) return {true, 0, l.as_f() * r.as_f()};
        return {false, l.i * r.i, 0};
      }
      if (f) return {true, 0, r.as_f() == 0 ? NAN : l.as_f() / r.as_f()};
      return {false, r.i == 0 ? 0 : l.i / r.i, 0};
    }
    if (n == "neg") {
      EvalNum x = eval_num(v.args[0], b, row);
      if (x.is_f) return {true, 0, -x.d};
      return {false, -x.i, 0};
    }
    if (n == "if") {
      if (v.args.size() < 3) throw std::runtime_error("if arity");
      if (eval_pred(v.args[0], b, row)) return eval_num(v.args[1], b, row);
      return eval_num(v.args[2], b, row);
    }
  }
  throw std::runtime_error("cannot eval numeric");
}

bool eval_pred(const Val &v, const Batch &b, int row) {
  if (v.kind == ValKind::BOOL) return v.b;
  if (v.kind == ValKind::COL) return b.get_b(static_cast<int>(v.i), row);
  if (v.kind != ValKind::CALL) return eval_num(v, b, row).as_i() != 0;
  const auto &n = v.name;
  if (n == "and") return eval_pred(v.args[0], b, row) && eval_pred(v.args[1], b, row);
  if (n == "or") return eval_pred(v.args[0], b, row) || eval_pred(v.args[1], b, row);
  EvalNum l = eval_num(v.args[0], b, row);
  EvalNum r = eval_num(v.args[1], b, row);
  const bool f = l.is_f || r.is_f;
  const double ld = l.as_f(), rd = r.as_f();
  if (n == "eq") return f ? ld == rd : l.as_i() == r.as_i();
  if (n == "ne") return f ? ld != rd : l.as_i() != r.as_i();
  if (n == "gt") return f ? ld > rd : l.as_i() > r.as_i();
  if (n == "ge") return f ? ld >= rd : l.as_i() >= r.as_i();
  if (n == "lt") return f ? ld < rd : l.as_i() < r.as_i();
  if (n == "le") return f ? ld <= rd : l.as_i() <= r.as_i();
  throw std::runtime_error("bad pred " + n);
}

Batch project_cols(const Batch &in, const std::vector<Val> &exprs) {
  Batch out;
  out.n_rows = in.n_rows;
  for (const auto &e : exprs) {
    NsType t = NS_I64;
    if (e.kind == ValKind::F64) t = NS_F64;
    else if (e.kind == ValKind::BOOL) t = NS_BOOL;
    else if (e.kind == ValKind::COL) {
      const int c = static_cast<int>(e.i);
      t = (c >= 0 && c < in.n_cols()) ? in.types[c] : NS_I64;
    }
    else if (e.kind == ValKind::CALL) {
      /* arithmetic stays numeric; avg later */
      t = NS_I64;
      if (!e.args.empty()) {
        EvalNum probe = eval_num(e, in, 0);
        if (probe.is_f) t = NS_F64;
      }
    }
    if (in.n_rows == 0 && e.kind == ValKind::CALL) t = NS_I64;
    out.add_col(t, in.n_rows);
    const int oc = out.n_cols() - 1;
    for (int r = 0; r < in.n_rows; ++r) {
      if (t == NS_BOOL) out.b[oc][r] = eval_pred(e, in, r) ? 1 : 0;
      else {
        EvalNum n = eval_num(e, in, r);
        if (t == NS_F64) out.f64[oc][r] = n.as_f();
        else out.i64[oc][r] = n.as_i();
      }
    }
  }
  return out;
}

bool pred_col_lit(const Val &pred, int *col, int64_t *lo, int64_t *hi, bool *eq_only) {
  *eq_only = false;
  if (pred.kind != ValKind::CALL || pred.args.size() < 2) return false;
  if (pred.name == "and") {
    int c0 = -1, c1 = -1;
    int64_t lo0, hi0, lo1, hi1;
    bool e0, e1;
    if (!pred_col_lit(pred.args[0], &c0, &lo0, &hi0, &e0)) return false;
    if (!pred_col_lit(pred.args[1], &c1, &lo1, &hi1, &e1)) return false;
    if (c0 != c1) return false;
    *col = c0;
    *lo = lo0 > lo1 ? lo0 : lo1;
    *hi = hi0 < hi1 ? hi0 : hi1;
    *eq_only = *lo == *hi;
    return true;
  }
  const Val *c = nullptr;
  const Val *lit = nullptr;
  bool flip = false;
  if (pred.args[0].kind == ValKind::COL && pred.args[1].kind == ValKind::I64) {
    c = &pred.args[0];
    lit = &pred.args[1];
  } else if (pred.args[1].kind == ValKind::COL && pred.args[0].kind == ValKind::I64) {
    c = &pred.args[1];
    lit = &pred.args[0];
    flip = true;
  } else {
    return false;
  }
  *col = static_cast<int>(c->i);
  const int64_t v = lit->i;
  if (pred.name == "eq") {
    *lo = v;
    *hi = v;
    *eq_only = true;
    return true;
  }
  if (pred.name == "ge") {
    if (flip) { *lo = INT64_MIN; *hi = v; }
    else { *lo = v; *hi = INT64_MAX; }
    return true;
  }
  if (pred.name == "le") {
    if (flip) { *lo = v; *hi = INT64_MAX; }
    else { *lo = INT64_MIN; *hi = v; }
    return true;
  }
  if (pred.name == "gt") {
    if (flip) { *lo = INT64_MIN; *hi = v - 1; }
    else { *lo = v + 1; *hi = INT64_MAX; }
    return true;
  }
  if (pred.name == "lt") {
    if (flip) { *lo = v + 1; *hi = INT64_MAX; }
    else { *lo = INT64_MIN; *hi = v - 1; }
    return true;
  }
  return false;
}

Batch do_filter(Batch in, const Val &pred) {
  int col = -1;
  int64_t lo = 0, hi = 0;
  bool eq_only = false;
  if (pred_col_lit(pred, &col, &lo, &hi, &eq_only) &&
      col >= 0 && col < in.n_cols() && in.types[col] != NS_F64) {
    const int64_t *src = in.i64[col].data();
    int w = 0;
    for (int r = 0; r < in.n_rows; ++r) {
      const int64_t v = src[r];
      if (v < lo || v > hi) continue;
      if (w != r) {
        for (int c = 0; c < in.n_cols(); ++c) {
          if (in.types[c] == NS_F64) in.f64[c][w] = in.f64[c][r];
          else if (in.types[c] == NS_BOOL) in.b[c][w] = in.b[c][r];
          else in.i64[c][w] = in.i64[c][r];
        }
      }
      w += 1;
    }
    in.n_rows = w;
    for (int c = 0; c < in.n_cols(); ++c) {
      if (in.types[c] == NS_F64) in.f64[c].resize(static_cast<size_t>(w));
      else if (in.types[c] == NS_BOOL) in.b[c].resize(static_cast<size_t>(w));
      else in.i64[c].resize(static_cast<size_t>(w));
    }
    return in;
  }
  int w = 0;
  for (int r = 0; r < in.n_rows; ++r) {
    if (!eval_pred(pred, in, r)) continue;
    if (w != r) {
      for (int c = 0; c < in.n_cols(); ++c) {
        if (in.types[c] == NS_F64) in.f64[c][w] = in.f64[c][r];
        else if (in.types[c] == NS_BOOL) in.b[c][w] = in.b[c][r];
        else in.i64[c][w] = in.i64[c][r];
      }
    }
    w += 1;
  }
  in.n_rows = w;
  for (int c = 0; c < in.n_cols(); ++c) {
    if (in.types[c] == NS_F64) in.f64[c].resize(static_cast<size_t>(w));
    else if (in.types[c] == NS_BOOL) in.b[c].resize(static_cast<size_t>(w));
    else in.i64[c].resize(static_cast<size_t>(w));
  }
  return in;
}

static uint64_t mix(uint64_t x) {
  x ^= x >> 30;
  x *= 0xbf58476d1ce4e5b9ULL;
  x ^= x >> 27;
  x *= 0x94d049bb133111ebULL;
  x ^= x >> 31;
  return x;
}

struct OpenHash {
  struct Slot {
    uint64_t key = 0;
    int idx = -1;
    bool used = false;
  };
  std::vector<Slot> t;
  explicit OpenHash(size_t n) {
    size_t cap = 8;
    while (cap < n * 2 + 8) cap <<= 1;
    t.assign(cap, Slot{});
  }
  int find(uint64_t k) const {
    const size_t m = t.size() - 1;
    size_t i = mix(k) & m;
    for (size_t n = 0; n < t.size(); ++n) {
      if (!t[i].used) return -1;
      if (t[i].key == k) return t[i].idx;
      i = (i + 1) & m;
    }
    return -1;
  }
  int insert(uint64_t k, int idx) {
    const size_t m = t.size() - 1;
    size_t i = mix(k) & m;
    for (size_t n = 0; n < t.size(); ++n) {
      if (!t[i].used) {
        t[i].used = true;
        t[i].key = k;
        t[i].idx = idx;
        return idx;
      }
      if (t[i].key == k) return t[i].idx;
      i = (i + 1) & m;
    }
    throw std::runtime_error("hash full");
  }
};

const int64_t *i64_ptr(const Val &v, const Batch &b) {
  if (v.kind != ValKind::COL) return nullptr;
  const int c = static_cast<int>(v.i);
  if (c < 0 || c >= b.n_cols() || b.types[c] == NS_F64) return nullptr;
  return b.i64[c].data();
}

const double *f64_ptr(const Val &v, const Batch &b) {
  if (v.kind != ValKind::COL) return nullptr;
  const int c = static_cast<int>(v.i);
  if (c < 0 || c >= b.n_cols() || b.types[c] != NS_F64) return nullptr;
  return b.f64[c].data();
}

uint64_t mix_key(int64_t v) {
  uint64_t h = 1469598103934665603ULL;
  h ^= mix(static_cast<uint64_t>(v));
  h *= 1099511628211ULL;
  return h;
}

uint64_t row_key(const Batch &b, const std::vector<Val> &keys, int row) {
  if (keys.size() == 1) {
    const int64_t *p = i64_ptr(keys[0], b);
    if (p != nullptr) return mix_key(p[row]);
  }
  uint64_t h = 1469598103934665603ULL;
  for (const auto &k : keys) {
    const int64_t v = eval_num(k, b, row).as_i();
    h ^= mix(static_cast<uint64_t>(v));
    h *= 1099511628211ULL;
  }
  return h;
}

Batch do_hashagg(const Batch &in, const std::vector<Val> &keys, const std::vector<Val> &aggs) {
  OpenHash ht(static_cast<size_t>(in.n_rows) + 8);
  std::vector<int> first;
  struct Acc {
    int64_t cnt = 0;
    int64_t isum = 0;
    double fsum = 0;
    bool is_f = false;
    int64_t imin = 0, imax = 0;
    double fmin = 0, fmax = 0;
    bool init = false;
  };
  std::vector<std::vector<Acc>> accs;
  const int64_t *key_p = (keys.size() == 1) ? i64_ptr(keys[0], in) : nullptr;
  for (int r = 0; r < in.n_rows; ++r) {
    const uint64_t k = key_p != nullptr ? mix_key(key_p[r]) : row_key(in, keys, r);
    int g = ht.find(k);
    if (g < 0) {
      g = static_cast<int>(first.size());
      ht.insert(k, g);
      first.push_back(r);
      accs.emplace_back(aggs.size());
    }
    for (size_t a = 0; a < aggs.size(); ++a) {
      Acc &ac = accs[g][a];
      const Val &fn = aggs[a];
      if (fn.kind != ValKind::CALL) continue;
      if (fn.name == "count") {
        ac.cnt += 1;
        continue;
      }
      const int64_t *ip = fn.args.empty() ? nullptr : i64_ptr(fn.args[0], in);
      const double *fp = fn.args.empty() ? nullptr : f64_ptr(fn.args[0], in);
      EvalNum x;
      if (fp != nullptr) {
        x = {true, 0, fp[r]};
      } else if (ip != nullptr) {
        x = {false, ip[r], 0};
      } else {
        x = eval_num(fn.args[0], in, r);
      }
      if (!ac.init) {
        ac.init = true;
        ac.is_f = x.is_f;
        ac.imin = ac.imax = x.as_i();
        ac.fmin = ac.fmax = x.as_f();
      }
      if (fn.name == "sum" || fn.name == "avg") {
        ac.cnt += 1;
        if (x.is_f || ac.is_f) {
          ac.is_f = true;
          ac.fsum += x.as_f();
        } else ac.isum += x.i;
      } else if (fn.name == "min") {
        if (x.is_f || ac.is_f) {
          ac.is_f = true;
          ac.fmin = std::min(ac.fmin, x.as_f());
        } else ac.imin = std::min(ac.imin, x.i);
      } else if (fn.name == "max") {
        if (x.is_f || ac.is_f) {
          ac.is_f = true;
          ac.fmax = std::max(ac.fmax, x.as_f());
        } else ac.imax = std::max(ac.imax, x.i);
      }
    }
  }

  const int ng = static_cast<int>(first.size());
  Batch out;
  out.n_rows = ng;
  /* output columns follow agg list only (Spark result expressions) */
  for (size_t a = 0; a < aggs.size(); ++a) {
    const Val &fn = aggs[a];
    NsType t = NS_I64;
    if (fn.kind == ValKind::COL) t = in.types[static_cast<int>(fn.i)];
    else if (fn.kind == ValKind::CALL && (fn.name == "avg" ||
             (!fn.args.empty() && fn.args[0].kind == ValKind::COL &&
              in.types[static_cast<int>(fn.args[0].i)] == NS_F64))) {
      t = NS_F64;
    } else if (fn.kind == ValKind::CALL && fn.name == "avg") t = NS_F64;
    else if (fn.kind != ValKind::CALL) {
      /* grouping key passed through as col */
    }
    if (fn.kind != ValKind::CALL) {
      /* e.g. key attribute in the agg list */
      if (fn.kind == ValKind::COL) t = in.types[static_cast<int>(fn.i)];
    }
    out.add_col(t, ng);
    for (int g = 0; g < ng; ++g) {
      if (fn.kind != ValKind::CALL) {
        EvalNum n = eval_num(fn, in, first[g]);
        if (t == NS_F64) out.f64[a][g] = n.as_f();
        else if (t == NS_BOOL) out.b[a][g] = n.as_i() ? 1 : 0;
        else out.i64[a][g] = n.as_i();
        continue;
      }
      const Acc &ac = accs[g][a];
      if (fn.name == "count") out.i64[a][g] = ac.cnt;
      else if (fn.name == "sum") {
        if (t == NS_F64) out.f64[a][g] = ac.is_f ? ac.fsum : static_cast<double>(ac.isum);
        else out.i64[a][g] = ac.isum;
      } else if (fn.name == "avg") {
        out.f64[a][g] = ac.cnt ? (ac.is_f ? ac.fsum : static_cast<double>(ac.isum)) / ac.cnt : NAN;
      } else if (fn.name == "min") {
        if (t == NS_F64) out.f64[a][g] = ac.fmin;
        else out.i64[a][g] = ac.imin;
      } else if (fn.name == "max") {
        if (t == NS_F64) out.f64[a][g] = ac.fmax;
        else out.i64[a][g] = ac.imax;
      }
    }
  }
  return out;
}

/* One row: for each [lo,hi] segment, the agg list. Rows outside every
 * segment are dropped. Used for Q9-style multi-bucket scans. */
Batch do_segagg(const Batch &in, const Val &key, const std::vector<Val> &bounds,
                const std::vector<Val> &aggs) {
  const int nseg = static_cast<int>(bounds.size() / 2);
  if (nseg <= 0) throw std::runtime_error("segagg bounds");
  struct Acc {
    int64_t cnt = 0;
    int64_t isum = 0;
    double fsum = 0;
    bool is_f = false;
    int64_t imin = 0, imax = 0;
    double fmin = 0, fmax = 0;
    bool init = false;
  };
  std::vector<int64_t> lo(static_cast<size_t>(nseg)), hi(static_cast<size_t>(nseg));
  for (int s = 0; s < nseg; ++s) {
    lo[static_cast<size_t>(s)] = bounds[static_cast<size_t>(s) * 2].i;
    hi[static_cast<size_t>(s)] = bounds[static_cast<size_t>(s) * 2 + 1].i;
  }
  std::vector<std::vector<Acc>> accs(static_cast<size_t>(nseg), std::vector<Acc>(aggs.size()));
  const int64_t *kp = i64_ptr(key, in);
  for (int r = 0; r < in.n_rows; ++r) {
    const int64_t kv = kp != nullptr ? kp[r] : eval_num(key, in, r).as_i();
    int seg = -1;
    for (int s = 0; s < nseg; ++s) {
      if (kv >= lo[static_cast<size_t>(s)] && kv <= hi[static_cast<size_t>(s)]) {
        seg = s;
        break;
      }
    }
    if (seg < 0) continue;
    for (size_t a = 0; a < aggs.size(); ++a) {
      Acc &ac = accs[static_cast<size_t>(seg)][a];
      const Val &fn = aggs[a];
      if (fn.kind != ValKind::CALL) continue;
      if (fn.name == "count") {
        ac.cnt += 1;
        continue;
      }
      const int64_t *ip = fn.args.empty() ? nullptr : i64_ptr(fn.args[0], in);
      const double *fp = fn.args.empty() ? nullptr : f64_ptr(fn.args[0], in);
      EvalNum x;
      if (fp != nullptr) {
        x = {true, 0, fp[r]};
      } else if (ip != nullptr) {
        x = {false, ip[r], 0};
      } else {
        x = eval_num(fn.args[0], in, r);
      }
      if (!ac.init) {
        ac.init = true;
        ac.is_f = x.is_f;
        ac.imin = ac.imax = x.as_i();
        ac.fmin = ac.fmax = x.as_f();
      }
      if (fn.name == "sum" || fn.name == "avg") {
        ac.cnt += 1;
        if (x.is_f || ac.is_f) {
          ac.is_f = true;
          ac.fsum += x.as_f();
        } else {
          ac.isum += x.i;
        }
      }
    }
  }
  Batch out;
  out.n_rows = 1;
  const int ncols = nseg * static_cast<int>(aggs.size());
  for (int s = 0; s < nseg; ++s) {
    for (size_t a = 0; a < aggs.size(); ++a) {
      const Val &fn = aggs[a];
      NsType t = NS_I64;
      if (fn.kind == ValKind::CALL && (fn.name == "avg" ||
          (!fn.args.empty() && fn.args[0].kind == ValKind::COL &&
           in.types[static_cast<int>(fn.args[0].i)] == NS_F64))) {
        t = NS_F64;
      }
      out.add_col(t, 1);
      const int oc = out.n_cols() - 1;
      const Acc &ac = accs[static_cast<size_t>(s)][a];
      if (fn.name == "count") {
        out.i64[oc][0] = ac.cnt;
      } else if (fn.name == "sum") {
        if (t == NS_F64) out.f64[oc][0] = ac.is_f ? ac.fsum : static_cast<double>(ac.isum);
        else out.i64[oc][0] = ac.isum;
      } else if (fn.name == "avg") {
        out.f64[oc][0] =
            ac.cnt ? (ac.is_f ? ac.fsum : static_cast<double>(ac.isum)) / ac.cnt : NAN;
      } else {
        out.i64[oc][0] = 0;
      }
    }
  }
  (void)ncols;
  return out;
}

void append_batch(Batch *dst, const Batch &src) {
  if (dst->n_cols() == 0) {
    *dst = src;
    return;
  }
  if (dst->n_cols() != src.n_cols()) throw std::runtime_error("append width");
  const int old = dst->n_rows;
  dst->n_rows = old + src.n_rows;
  for (int c = 0; c < dst->n_cols(); ++c) {
    if (dst->types[c] == NS_F64) {
      dst->f64[c].insert(dst->f64[c].end(), src.f64[c].begin(), src.f64[c].end());
    } else if (dst->types[c] == NS_BOOL) {
      dst->b[c].insert(dst->b[c].end(), src.b[c].begin(), src.b[c].end());
    } else {
      dst->i64[c].insert(dst->i64[c].end(), src.i64[c].begin(), src.i64[c].end());
    }
  }
}

Batch slice_rows(const Batch &in, int start, int n) {
  Batch o;
  if (start < 0) start = 0;
  if (n < 0) n = 0;
  if (start > in.n_rows) start = in.n_rows;
  if (start + n > in.n_rows) n = in.n_rows - start;
  o.n_rows = n;
  for (int c = 0; c < in.n_cols(); ++c) {
    o.add_col(in.types[c], n);
    if (n <= 0) continue;
    if (in.types[c] == NS_F64) {
      std::memcpy(o.f64[c].data(), in.f64[c].data() + start, sizeof(double) * static_cast<size_t>(n));
    } else if (in.types[c] == NS_BOOL) {
      std::memcpy(o.b[c].data(), in.b[c].data() + start, static_cast<size_t>(n));
    } else {
      std::memcpy(o.i64[c].data(), in.i64[c].data() + start,
                  sizeof(int64_t) * static_cast<size_t>(n));
    }
  }
  return o;
}

const int kJoinTile = 262144;

void build_i64_join(
    const int64_t *keys, int n, OpenHash *ht, std::vector<std::vector<int>> *buckets,
    int *next) {
  for (int r = 0; r < n; ++r) {
    const uint64_t k = mix(static_cast<uint64_t>(keys[r]));
    int g = ht->find(k);
    if (g < 0) {
      g = (*next)++;
      ht->insert(k, g);
    }
    (*buckets)[static_cast<size_t>(g)].push_back(r);
  }
}

Batch do_join_core(const Batch &left, const Batch &right, const Val &lk, const Val &rk) {
  OpenHash ht(static_cast<size_t>(right.n_rows) + 8);
  std::vector<std::vector<int>> buckets(right.n_rows + 1);
  int next = 0;
  const int64_t *rkp = i64_ptr(rk, right);
  const int64_t *lkp = i64_ptr(lk, left);
  if (rkp != nullptr) {
    build_i64_join(rkp, right.n_rows, &ht, &buckets, &next);
  } else {
    for (int r = 0; r < right.n_rows; ++r) {
      const uint64_t k = mix(static_cast<uint64_t>(eval_num(rk, right, r).as_i()));
      int g = ht.find(k);
      if (g < 0) {
        g = next++;
        ht.insert(k, g);
      }
      buckets[g].push_back(r);
    }
  }
  std::vector<int> lr, rr;
  for (int l = 0; l < left.n_rows; ++l) {
    const int64_t kv = lkp != nullptr ? lkp[l] : eval_num(lk, left, l).as_i();
    const int g = ht.find(mix(static_cast<uint64_t>(kv)));
    if (g < 0) continue;
    for (int r : buckets[g]) {
      const int64_t rv = rkp != nullptr ? rkp[r] : eval_num(rk, right, r).as_i();
      if (kv == rv) {
        lr.push_back(l);
        rr.push_back(r);
        const size_t cells =
            lr.size() * static_cast<size_t>(left.n_cols() + right.n_cols() + 1);
        if (lr.size() > 20000000u || cells > 80000000ull) {
          throw std::runtime_error("hashjoin output cap");
        }
      }
    }
  }
  Batch out;
  out.n_rows = static_cast<int>(lr.size());
  for (int c = 0; c < left.n_cols(); ++c) {
    out.add_col(left.types[c], out.n_rows);
    for (int i = 0; i < out.n_rows; ++i) {
      const int r = lr[i];
      if (left.types[c] == NS_F64) out.f64[out.n_cols() - 1][i] = left.f64[c][r];
      else if (left.types[c] == NS_BOOL) out.b[out.n_cols() - 1][i] = left.b[c][r];
      else out.i64[out.n_cols() - 1][i] = left.i64[c][r];
    }
  }
  for (int c = 0; c < right.n_cols(); ++c) {
    out.add_col(right.types[c], out.n_rows);
    for (int i = 0; i < out.n_rows; ++i) {
      const int r = rr[i];
      if (right.types[c] == NS_F64) out.f64[out.n_cols() - 1][i] = right.f64[c][r];
      else if (right.types[c] == NS_BOOL) out.b[out.n_cols() - 1][i] = right.b[c][r];
      else out.i64[out.n_cols() - 1][i] = right.i64[c][r];
    }
  }
  return out;
}

Batch do_join(const Batch &left, const Batch &right, const Val &lk, const Val &rk) {
  if (left.n_rows > kJoinTile) {
    Batch acc;
    for (int off = 0; off < left.n_rows; off += kJoinTile) {
      const int n = left.n_rows - off < kJoinTile ? left.n_rows - off : kJoinTile;
      Batch chunk = do_join_core(slice_rows(left, off, n), right, lk, rk);
      if (off == 0) acc = std::move(chunk);
      else append_batch(&acc, chunk);
    }
    return acc;
  }
  return do_join_core(left, right, lk, rk);
}

/* Join but emit only projected columns of the concatenated (left|right) row. */
void collect_join_pairs(
    const Batch &left, const Batch &right, const Val &lk, const Val &rk,
    OpenHash *ht, const std::vector<std::vector<int>> *buckets,
    std::vector<int> *lr, std::vector<int> *rr, size_t width) {
  const int64_t *lkp = i64_ptr(lk, left);
  const int64_t *rkp = i64_ptr(rk, right);
  for (int l = 0; l < left.n_rows; ++l) {
    const int64_t kv = lkp != nullptr ? lkp[l] : eval_num(lk, left, l).as_i();
    const int g = ht->find(mix(static_cast<uint64_t>(kv)));
    if (g < 0) continue;
    for (int r : (*buckets)[static_cast<size_t>(g)]) {
      const int64_t rv = rkp != nullptr ? rkp[r] : eval_num(rk, right, r).as_i();
      if (kv == rv) {
        lr->push_back(l);
        rr->push_back(r);
        const size_t cells = lr->size() * (width + 1);
        if (lr->size() > 20000000u || cells > 80000000ull) {
          throw std::runtime_error("hashjoin output cap");
        }
      }
    }
  }
}

void copy_join_col(
    const Batch &src, int src_c, const std::vector<int> &idx, Batch *out) {
  out->add_col(src.types[src_c], static_cast<int>(idx.size()));
  const int oc = out->n_cols() - 1;
  for (int i = 0; i < out->n_rows; ++i) {
    const int r = idx[static_cast<size_t>(i)];
    if (src.types[src_c] == NS_F64) out->f64[oc][i] = src.f64[src_c][r];
    else if (src.types[src_c] == NS_BOOL) out->b[oc][i] = src.b[src_c][r];
    else out->i64[oc][i] = src.i64[src_c][r];
  }
}

Batch do_join_project(
    const Batch &left, const Batch &right, const Val &lk, const Val &rk,
    const std::vector<Val> &projs) {
  if (projs.empty()) return do_join(left, right, lk, rk);
  for (const auto &e : projs) {
    if (e.kind != ValKind::COL) return project_cols(do_join(left, right, lk, rk), projs);
  }
  OpenHash ht(static_cast<size_t>(right.n_rows) + 8);
  std::vector<std::vector<int>> buckets(static_cast<size_t>(right.n_rows) + 1);
  int next = 0;
  const int64_t *rkp = i64_ptr(rk, right);
  if (rkp != nullptr) {
    build_i64_join(rkp, right.n_rows, &ht, &buckets, &next);
  } else {
    for (int r = 0; r < right.n_rows; ++r) {
      const uint64_t k = mix(static_cast<uint64_t>(eval_num(rk, right, r).as_i()));
      int g = ht.find(k);
      if (g < 0) {
        g = next++;
        ht.insert(k, g);
      }
      buckets[static_cast<size_t>(g)].push_back(r);
    }
  }
  Batch acc;
  const size_t width = projs.size();
  auto one_tile = [&](const Batch &probe) {
    std::vector<int> lr, rr;
    collect_join_pairs(probe, right, lk, rk, &ht, &buckets, &lr, &rr, width);
    Batch chunk;
    chunk.n_rows = static_cast<int>(lr.size());
    for (const auto &e : projs) {
      const int c = static_cast<int>(e.i);
      if (c < probe.n_cols()) {
        copy_join_col(probe, c, lr, &chunk);
      } else {
        const int rc = c - probe.n_cols();
        if (rc < 0 || rc >= right.n_cols()) {
          chunk.add_col(NS_I64, chunk.n_rows);
        } else {
          copy_join_col(right, rc, rr, &chunk);
        }
      }
    }
    return chunk;
  };
  if (left.n_rows > kJoinTile) {
    for (int off = 0; off < left.n_rows; off += kJoinTile) {
      const int n = left.n_rows - off < kJoinTile ? left.n_rows - off : kJoinTile;
      Batch chunk = one_tile(slice_rows(left, off, n));
      if (off == 0) acc = std::move(chunk);
      else append_batch(&acc, chunk);
    }
    return acc;
  }
  return one_tile(left);
}

/* Row-id join: two i32 columns (left_idx, right_idx) so Spark can gather
 * original payload including strings without a C++ copy. */
Batch do_join_idx(const Batch &left, const Batch &right, const Val &lk, const Val &rk) {
  OpenHash ht(static_cast<size_t>(right.n_rows) + 8);
  std::vector<std::vector<int>> buckets(right.n_rows + 1);
  int next = 0;
  const int64_t *rkp = i64_ptr(rk, right);
  const int64_t *lkp = i64_ptr(lk, left);
  if (rkp != nullptr) {
    build_i64_join(rkp, right.n_rows, &ht, &buckets, &next);
  } else {
    for (int r = 0; r < right.n_rows; ++r) {
      const uint64_t k = mix(static_cast<uint64_t>(eval_num(rk, right, r).as_i()));
      int g = ht.find(k);
      if (g < 0) {
        g = next++;
        ht.insert(k, g);
      }
      buckets[g].push_back(r);
    }
  }
  std::vector<int> lr, rr;
  for (int l = 0; l < left.n_rows; ++l) {
    const int64_t kv = lkp != nullptr ? lkp[l] : eval_num(lk, left, l).as_i();
    const int g = ht.find(mix(static_cast<uint64_t>(kv)));
    if (g < 0) continue;
    for (int r : buckets[g]) {
      const int64_t rv = rkp != nullptr ? rkp[r] : eval_num(rk, right, r).as_i();
      if (kv == rv) {
        lr.push_back(l);
        rr.push_back(r);
        if (lr.size() > 20000000u) {
          throw std::runtime_error("hashjoin output cap");
        }
      }
    }
  }
  Batch out;
  out.n_rows = static_cast<int>(lr.size());
  out.add_col(NS_I32, out.n_rows);
  out.add_col(NS_I32, out.n_rows);
  for (int i = 0; i < out.n_rows; ++i) {
    out.i64[0][i] = lr[static_cast<size_t>(i)];
    out.i64[1][i] = rr[static_cast<size_t>(i)];
  }
  return out;
}

Batch do_semi(const Batch &left, const Batch &right, const Val &lk, const Val &rk) {
  OpenHash ht(static_cast<size_t>(right.n_rows) + 8);
  const int64_t *rkp = i64_ptr(rk, right);
  const int64_t *lkp = i64_ptr(lk, left);
  if (rkp != nullptr) {
    for (int r = 0; r < right.n_rows; ++r) {
      const uint64_t k = static_cast<uint64_t>(rkp[r]);
      if (ht.find(k) < 0) ht.insert(k, r);
    }
  } else {
    for (int r = 0; r < right.n_rows; ++r) {
      const uint64_t k = static_cast<uint64_t>(eval_num(rk, right, r).as_i());
      if (ht.find(k) < 0) ht.insert(k, r);
    }
  }
  std::vector<int> keep;
  keep.reserve(static_cast<size_t>(left.n_rows));
  if (lkp != nullptr) {
    for (int l = 0; l < left.n_rows; ++l) {
      if (ht.find(static_cast<uint64_t>(lkp[l])) >= 0) keep.push_back(l);
    }
  } else {
    for (int l = 0; l < left.n_rows; ++l) {
      const uint64_t k = static_cast<uint64_t>(eval_num(lk, left, l).as_i());
      if (ht.find(k) >= 0) keep.push_back(l);
    }
  }
  Batch out;
  out.n_rows = static_cast<int>(keep.size());
  for (int c = 0; c < left.n_cols(); ++c) {
    out.add_col(left.types[c], out.n_rows);
    for (int i = 0; i < out.n_rows; ++i) {
      const int r = keep[static_cast<size_t>(i)];
      if (left.types[c] == NS_F64) out.f64[out.n_cols() - 1][i] = left.f64[c][r];
      else if (left.types[c] == NS_BOOL) out.b[out.n_cols() - 1][i] = left.b[c][r];
      else out.i64[out.n_cols() - 1][i] = left.i64[c][r];
    }
  }
  return out;
}

Batch do_union(const Batch &a, const Batch &b) {
  if (a.n_cols() != b.n_cols()) throw std::runtime_error("union width");
  Batch out;
  out.n_rows = a.n_rows + b.n_rows;
  for (int c = 0; c < a.n_cols(); ++c) {
    NsType t = a.types[c];
    if (c < b.n_cols() && b.types[c] == NS_F64) t = NS_F64;
    out.add_col(t, out.n_rows);
    for (int i = 0; i < a.n_rows; ++i) {
      if (t == NS_F64) {
        out.f64[c][i] = a.types[c] == NS_F64 ? a.f64[c][i] : static_cast<double>(a.i64[c][i]);
      } else if (t == NS_BOOL) {
        out.b[c][i] = a.b[c][i];
      } else {
        out.i64[c][i] = a.i64[c][i];
      }
    }
    for (int i = 0; i < b.n_rows; ++i) {
      const int d = a.n_rows + i;
      if (t == NS_F64) {
        out.f64[c][d] = b.types[c] == NS_F64 ? b.f64[c][i] : static_cast<double>(b.i64[c][i]);
      } else if (t == NS_BOOL) {
        out.b[c][d] = b.b[c][i];
      } else {
        out.i64[c][d] = b.i64[c][i];
      }
    }
  }
  return out;
}

Batch apply_perm(const Batch &in, const std::vector<uint32_t> &perm) {
  Batch out;
  out.n_rows = in.n_rows;
  for (int c = 0; c < in.n_cols(); ++c) {
    out.add_col(in.types[c], in.n_rows);
    for (int i = 0; i < in.n_rows; ++i) {
      const int r = static_cast<int>(perm[static_cast<size_t>(i)]);
      if (in.types[c] == NS_F64) out.f64[c][i] = in.f64[c][r];
      else if (in.types[c] == NS_BOOL) out.b[c][i] = in.b[c][r];
      else out.i64[c][i] = in.i64[c][r];
    }
  }
  return out;
}

Batch do_sort(const Batch &in, const std::vector<Val> &keys) {
  if (in.n_rows <= 1 || keys.empty()) return in;
  std::vector<NsCol> keycols;
  keycols.reserve(keys.size());
  for (const auto &k : keys) {
    if (k.kind != ValKind::COL) throw std::runtime_error("sort keys must be column refs");
    const int c = static_cast<int>(k.i);
    if (c < 0 || c >= in.n_cols()) throw std::runtime_error("sort key oob");
    NsCol col{};
    col.n_rows = in.n_rows;
    /* Batch stores i32/i64 in i64 vectors; expose as NS_I64. */
    if (in.types[c] == NS_F64) {
      col.type = NS_F64;
      col.data = const_cast<double *>(in.f64[c].data());
    } else if (in.types[c] == NS_BOOL) {
      col.type = NS_BOOL;
      col.data = const_cast<uint8_t *>(in.b[c].data());
    } else {
      col.type = NS_I64;
      col.data = const_cast<int64_t *>(in.i64[c].data());
    }
    keycols.push_back(col);
  }
  std::vector<uint32_t> perm(static_cast<size_t>(in.n_rows));
  if (ns_sort_permutation(keycols.data(), static_cast<int>(keycols.size()), in.n_rows,
                          perm.data()) != 0) {
    throw std::runtime_error("sort failed");
  }
  return apply_perm(in, perm);
}

Batch make_range(int64_t start, int64_t end, int64_t step) {
  if (step == 0) throw std::runtime_error("range step 0");
  int64_t n = 0;
  if ((end > start) == (step > 0)) {
    n = (end - start) / step;
    if ((end - start) % step != 0) n += 1;
    if (n < 0) n = 0;
  }
  if (n > INT32_MAX) n = INT32_MAX;
  Batch b;
  b.add_col(NS_I64, static_cast<int>(n));
  int64_t v = start;
  for (int i = 0; i < b.n_rows; ++i) {
    b.i64[0][i] = v;
    v += step;
  }
  return b;
}

Batch eval_plan(const Val &node, const NsBatch *inputs, int n_inputs);

thread_local std::vector<std::vector<NsColPred>> g_pushdown;
thread_local std::vector<NsColPred> g_merged_preds;

void collect_preds(const Val &pred, std::vector<NsColPred> *out) {
  if (pred.kind != ValKind::CALL || out == nullptr) return;
  if (pred.name == "and") {
    for (const auto &a : pred.args) collect_preds(a, out);
    return;
  }
  int32_t op = 0;
  if (pred.name == "eq") {
    op = 1;
  } else if (pred.name == "ge") {
    op = 2;
  } else if (pred.name == "le") {
    op = 3;
  } else if (pred.name == "gt") {
    op = 4;
  } else if (pred.name == "lt") {
    op = 5;
  } else {
    return;
  }
  if (pred.args.size() < 2) return;
  const Val *col = nullptr;
  const Val *lit = nullptr;
  bool flip = false;
  if (pred.args[0].kind == ValKind::COL && pred.args[1].kind == ValKind::I64) {
    col = &pred.args[0];
    lit = &pred.args[1];
  } else if (pred.args[1].kind == ValKind::COL && pred.args[0].kind == ValKind::I64) {
    col = &pred.args[1];
    lit = &pred.args[0];
    flip = true;
  } else {
    return;
  }
  if (flip) {
    if (op == 2) {
      op = 3;
    } else if (op == 3) {
      op = 2;
    } else if (op == 4) {
      op = 5;
    } else if (op == 5) {
      op = 4;
    }
  }
  NsColPred p{};
  p.col = static_cast<int32_t>(col->i);
  p.op = op;
  p.value = lit->i;
  out->push_back(p);
}

int direct_file_scan(const Val &node) {
  const Val *p = &node;
  while (p->kind == ValKind::CALL &&
         (p->name == "project" || p->name == "filter") && p->args.size() >= 2) {
    p = &p->args[1];
  }
  if (p->kind == ValKind::CALL && p->name == "scan" && !p->args.empty()) {
    return static_cast<int>(p->args[0].i);
  }
  return -1;
}

struct PredScope {
  int idx;
  std::vector<NsColPred> saved;
  PredScope(int i, const Val &pred) : idx(i) {
    if (idx < 0) return;
    if (g_pushdown.size() <= static_cast<size_t>(idx)) g_pushdown.resize(idx + 1);
    saved = g_pushdown[idx];
    collect_preds(pred, &g_pushdown[idx]);
  }
  ~PredScope() {
    if (idx < 0) return;
    if (static_cast<size_t>(idx) < g_pushdown.size()) g_pushdown[idx] = saved;
  }
};

NsFileScan attach_preds(int idx) {
  NsFileScan sc = g_file_scans[idx];
  g_merged_preds.clear();
  if (sc.preds != nullptr && sc.n_preds > 0) {
    g_merged_preds.insert(g_merged_preds.end(), sc.preds, sc.preds + sc.n_preds);
  }
  if (idx >= 0 && static_cast<size_t>(idx) < g_pushdown.size()) {
    g_merged_preds.insert(g_merged_preds.end(), g_pushdown[idx].begin(),
                          g_pushdown[idx].end());
  }
  if (!g_merged_preds.empty()) {
    sc.preds = g_merged_preds.data();
    sc.n_preds = static_cast<int32_t>(g_merged_preds.size());
  }
  return sc;
}

int file_n_splits(int idx) {
  if (!g_file_scans || idx < 0) return 0;
  return g_file_scans[idx].n_splits;
}

Batch read_file_split(int idx, int split) {
  if (!g_file_scans || idx < 0 || split < 0 || split >= g_file_scans[idx].n_splits) {
    throw std::runtime_error("split oob");
  }
  NsFileScan sc = attach_preds(idx);
  NsFileSplit one = sc.splits[split];
  sc.splits = &one;
  sc.n_splits = 1;
  NsBatch raw{};
  if (ns_parquet_read(&sc, &raw) != 0) throw std::runtime_error("parquet split failed");
  std::fprintf(stderr, "nativesql: from_c scan=%d split=%d rows=%d cols=%d\n",
               idx, split, raw.n_rows, raw.n_cols);
  std::fflush(stderr);
  Batch b = from_c(raw);
  ns_batch_free(&raw);
  std::fprintf(stderr, "nativesql: from_c done scan=%d split=%d\n", idx, split);
  std::fflush(stderr);
  return b;
}

int leftmost_file_scan(const Val &node) {
  const Val *p = &node;
  while (p->kind == ValKind::CALL &&
         (p->name == "project" || p->name == "filter") && p->args.size() >= 2) {
    p = &p->args[1];
  }
  if (p->kind == ValKind::CALL && p->name == "scan" && !p->args.empty()) {
    return static_cast<int>(p->args[0].i);
  }
  if (p->kind == ValKind::CALL && p->name == "hashjoin" && p->args.size() >= 4) {
    return leftmost_file_scan(p->args[2]);
  }
  return -1;
}

Batch eval_plan_probe_split(
    const Val &node, int probe_idx, int split, const NsBatch *inputs, int n_inputs) {
  if (node.kind != ValKind::CALL) throw std::runtime_error("plan must be a call");
  if (node.name == "scan") {
    const int idx = static_cast<int>(node.args[0].i);
    if (idx == probe_idx) return read_file_split(idx, split);
    return eval_plan(node, inputs, n_inputs);
  }
  if (node.name == "filter") {
    PredScope scope(direct_file_scan(node.args[1]), node.args[0]);
    return do_filter(
        eval_plan_probe_split(node.args[1], probe_idx, split, inputs, n_inputs),
        node.args[0]);
  }
  if (node.name == "project") {
    return project_cols(
        eval_plan_probe_split(node.args[1], probe_idx, split, inputs, n_inputs),
        node.args[0].args);
  }
  return eval_plan(node, inputs, n_inputs);
}

struct JoinLevel {
  Batch build;
  Val lk;
  Val rk;
  std::unique_ptr<OpenHash> ht;
  std::vector<std::vector<int>> buckets;
};

bool flatten_joins(const Val &node, Batch *probe, std::vector<JoinLevel> *levels,
                   const NsBatch *inputs, int n_inputs, const Batch *probe_override) {
  if (node.kind != ValKind::CALL) return false;
  if (node.name == "hashjoin" && node.args.size() >= 4) {
    if (!flatten_joins(node.args[2], probe, levels, inputs, n_inputs, probe_override)) {
      return false;
    }
    JoinLevel lv;
    lv.build = eval_plan(node.args[3], inputs, n_inputs);
    lv.lk = node.args[0];
    lv.rk = node.args[1];
    lv.ht = std::make_unique<OpenHash>(static_cast<size_t>(lv.build.n_rows) + 8);
    lv.buckets.resize(static_cast<size_t>(lv.build.n_rows) + 1);
    int next = 0;
    for (int r = 0; r < lv.build.n_rows; ++r) {
      const uint64_t k = mix(static_cast<uint64_t>(eval_num(lv.rk, lv.build, r).as_i()));
      int g = lv.ht->find(k);
      if (g < 0) {
        g = next++;
        lv.ht->insert(k, g);
      }
      lv.buckets[static_cast<size_t>(g)].push_back(r);
    }
    levels->push_back(std::move(lv));
    return true;
  }
  if (probe_override) {
    *probe = *probe_override;
  } else {
    *probe = eval_plan(node, inputs, n_inputs);
  }
  return true;
}

bool try_chunked_join(
    const Val &join, const std::vector<Val> *proj, const NsBatch *inputs, int n_inputs,
    Batch *out) {
  if (join.kind != ValKind::CALL || join.name != "hashjoin" || join.args.size() < 4) {
    return false;
  }
  const int pidx = leftmost_file_scan(join.args[2]);
  const int ns = file_n_splits(pidx);
  Batch right = eval_plan(join.args[3], inputs, n_inputs);
  auto one = [&](const Batch &left) -> Batch {
    std::fprintf(stderr, "nativesql: join left_rows=%d right_rows=%d left_cols=%d proj=%zu\n",
                 left.n_rows, right.n_rows, left.n_cols(), proj ? proj->size() : 0);
    std::fflush(stderr);
    Batch outj = proj ? do_join_project(left, right, join.args[0], join.args[1], *proj)
                      : do_join(left, right, join.args[0], join.args[1]);
    std::fprintf(stderr, "nativesql: join ok rows=%d cols=%d\n", outj.n_rows, outj.n_cols());
    std::fflush(stderr);
    return outj;
  };
  if (pidx >= 0 && ns > 1) {
    std::fprintf(stderr, "nativesql: chunked join probe_scan=%d splits=%d\n", pidx, ns);
    std::fflush(stderr);
    Batch acc;
    for (int s = 0; s < ns; ++s) {
      Batch left = eval_plan_probe_split(join.args[2], pidx, s, inputs, n_inputs);
      Batch chunk = one(left);
      if (s == 0) acc = std::move(chunk);
      else append_batch(&acc, chunk);
    }
    *out = std::move(acc);
    return true;
  }
  *out = one(eval_plan(join.args[2], inputs, n_inputs));
  return true;
}

struct JoinCtx {
  const Batch *probe = nullptr;
  int pr = 0;
  const std::vector<JoinLevel> *levels = nullptr;
  const std::vector<int> *br = nullptr;
};

int ctx_col(const JoinCtx &cx, int col, int *part, int *local) {
  if (col < 0) return -1;
  if (col < cx.probe->n_cols()) {
    *part = -1;
    *local = col;
    return 0;
  }
  int off = cx.probe->n_cols();
  for (size_t i = 0; i < cx.levels->size(); ++i) {
    const int n = (*cx.levels)[i].build.n_cols();
    if (col < off + n) {
      *part = static_cast<int>(i);
      *local = col - off;
      return 0;
    }
    off += n;
  }
  return -1;
}

int64_t ctx_get_i(const JoinCtx &cx, int col) {
  int part = -2, local = 0;
  if (ctx_col(cx, col, &part, &local) != 0) return 0;
  if (part < 0) return cx.probe->get_i(local, cx.pr);
  return (*cx.levels)[static_cast<size_t>(part)].build.get_i(local, (*cx.br)[static_cast<size_t>(part)]);
}

double ctx_get_f(const JoinCtx &cx, int col) {
  int part = -2, local = 0;
  if (ctx_col(cx, col, &part, &local) != 0) return 0;
  if (part < 0) return cx.probe->get_f(local, cx.pr);
  return (*cx.levels)[static_cast<size_t>(part)].build.get_f(local, (*cx.br)[static_cast<size_t>(part)]);
}

NsType ctx_type(const JoinCtx &cx, int col) {
  int part = -2, local = 0;
  if (ctx_col(cx, col, &part, &local) != 0) return NS_I64;
  if (part < 0) return cx.probe->types[static_cast<size_t>(local)];
  return (*cx.levels)[static_cast<size_t>(part)].build.types[static_cast<size_t>(local)];
}

EvalNum virt_eval_num(const Val &v, const JoinCtx &cx);

bool virt_eval_pred(const Val &v, const JoinCtx &cx) {
  if (v.kind == ValKind::BOOL) return v.b;
  if (v.kind == ValKind::COL) return ctx_get_i(cx, static_cast<int>(v.i)) != 0;
  if (v.kind != ValKind::CALL) return virt_eval_num(v, cx).as_i() != 0;
  if (v.name == "and") return virt_eval_pred(v.args[0], cx) && virt_eval_pred(v.args[1], cx);
  if (v.name == "or") return virt_eval_pred(v.args[0], cx) || virt_eval_pred(v.args[1], cx);
  EvalNum l = virt_eval_num(v.args[0], cx);
  EvalNum r = virt_eval_num(v.args[1], cx);
  const bool f = l.is_f || r.is_f;
  if (v.name == "eq") return f ? l.as_f() == r.as_f() : l.as_i() == r.as_i();
  if (v.name == "ne") return f ? l.as_f() != r.as_f() : l.as_i() != r.as_i();
  if (v.name == "gt") return f ? l.as_f() > r.as_f() : l.as_i() > r.as_i();
  if (v.name == "ge") return f ? l.as_f() >= r.as_f() : l.as_i() >= r.as_i();
  if (v.name == "lt") return f ? l.as_f() < r.as_f() : l.as_i() < r.as_i();
  if (v.name == "le") return f ? l.as_f() <= r.as_f() : l.as_i() <= r.as_i();
  return virt_eval_num(v, cx).as_i() != 0;
}

EvalNum virt_eval_num(const Val &v, const JoinCtx &cx) {
  if (v.kind == ValKind::I64) return {false, v.i, 0};
  if (v.kind == ValKind::F64) return {true, 0, v.d};
  if (v.kind == ValKind::BOOL) return {false, v.b ? 1 : 0, 0};
  if (v.kind == ValKind::COL) {
    const int c = static_cast<int>(v.i);
    if (ctx_type(cx, c) == NS_F64) return {true, 0, ctx_get_f(cx, c)};
    return {false, ctx_get_i(cx, c), 0};
  }
  if (v.kind == ValKind::CALL) {
    if (v.name == "add" || v.name == "sub" || v.name == "mul" || v.name == "div") {
      EvalNum l = virt_eval_num(v.args[0], cx);
      EvalNum r = virt_eval_num(v.args[1], cx);
      const bool f = l.is_f || r.is_f;
      if (v.name == "add") return f ? EvalNum{true, 0, l.as_f() + r.as_f()}
                                   : EvalNum{false, l.i + r.i, 0};
      if (v.name == "sub") return f ? EvalNum{true, 0, l.as_f() - r.as_f()}
                                   : EvalNum{false, l.i - r.i, 0};
      if (v.name == "mul") return f ? EvalNum{true, 0, l.as_f() * r.as_f()}
                                   : EvalNum{false, l.i * r.i, 0};
      return f ? EvalNum{true, 0, r.as_f() == 0 ? NAN : l.as_f() / r.as_f()}
               : EvalNum{false, r.i == 0 ? 0 : l.i / r.i, 0};
    }
    if (v.name == "neg") {
      EvalNum x = virt_eval_num(v.args[0], cx);
      return x.is_f ? EvalNum{true, 0, -x.d} : EvalNum{false, -x.i, 0};
    }
    if (v.name == "if" && v.args.size() >= 3) {
      return virt_eval_pred(v.args[0], cx) ? virt_eval_num(v.args[1], cx)
                                          : virt_eval_num(v.args[2], cx);
    }
  }
  return {false, 0, 0};
}

EvalNum proj_eval(const Val &v, const std::vector<Val> *proj, const JoinCtx &cx) {
  if (proj && v.kind == ValKind::COL && v.i >= 0 &&
      static_cast<size_t>(v.i) < proj->size()) {
    return virt_eval_num((*proj)[static_cast<size_t>(v.i)], cx);
  }
  return virt_eval_num(v, cx);
}

uint64_t virt_row_key(const std::vector<Val> &keys, const std::vector<Val> *proj,
                      const JoinCtx &cx) {
  uint64_t h = 1469598103934665603ULL;
  for (const auto &k : keys) {
    h ^= mix(static_cast<uint64_t>(proj_eval(k, proj, cx).as_i()));
    h *= 1099511628211ULL;
  }
  return h;
}

struct Acc {
  int64_t cnt = 0;
  int64_t isum = 0;
  double fsum = 0;
  bool is_f = false;
  int64_t imin = 0, imax = 0;
  double fmin = 0, fmax = 0;
  bool init = false;
};

void acc_update(Acc &ac, const Val &fn, const std::vector<Val> *proj, const JoinCtx &cx) {
  if (fn.kind != ValKind::CALL) return;
  if (fn.name == "count") {
    ac.cnt += 1;
    return;
  }
  EvalNum x = fn.args.empty() ? EvalNum{} : proj_eval(fn.args[0], proj, cx);
  if (!ac.init) {
    ac.init = true;
    ac.is_f = x.is_f;
    ac.imin = ac.imax = x.as_i();
    ac.fmin = ac.fmax = x.as_f();
  }
  if (fn.name == "sum" || fn.name == "avg") {
    ac.cnt += 1;
    if (x.is_f || ac.is_f) {
      ac.is_f = true;
      ac.fsum += x.as_f();
    } else {
      ac.isum += x.i;
    }
  } else if (fn.name == "min") {
    if (x.is_f || ac.is_f) {
      ac.is_f = true;
      ac.fmin = std::min(ac.fmin, x.as_f());
    } else {
      ac.imin = std::min(ac.imin, x.i);
    }
  } else if (fn.name == "max") {
    if (x.is_f || ac.is_f) {
      ac.is_f = true;
      ac.fmax = std::max(ac.fmax, x.as_f());
    } else {
      ac.imax = std::max(ac.imax, x.i);
    }
  }
}

bool try_fused_scan_hashagg(const Val &child, const std::vector<Val> &keys,
                            const std::vector<Val> &aggs, const NsBatch *inputs, int n_inputs,
                            Batch *out) {
  (void)inputs;
  (void)n_inputs;
  std::vector<Val> preds;
  const Val *cur = &child;
  while (cur->kind == ValKind::CALL && cur->name == "filter" && cur->args.size() >= 2) {
    preds.push_back(cur->args[0]);
    cur = &cur->args[1];
  }
  if (cur->kind != ValKind::CALL || cur->name != "scan" || cur->args.empty()) {
    return false;
  }
  const int idx = static_cast<int>(cur->args[0].i);
  const int ns = file_n_splits(idx);
  if (idx < 0 || ns != 1) return false;
  Batch b = read_file_split(idx, 0);
  for (int i = static_cast<int>(preds.size()) - 1; i >= 0; --i) {
    b = do_filter(std::move(b), preds[static_cast<size_t>(i)]);
  }
  *out = do_hashagg(b, keys, aggs);
  std::fprintf(stderr, "nativesql: fused scan-hashagg scan=%d rows=%d groups=%d\n",
               idx, b.n_rows, out->n_rows);
  std::fflush(stderr);
  return true;
}

bool try_fused_hashagg(const Val &child, const std::vector<Val> &keys,
                       const std::vector<Val> &aggs, const NsBatch *inputs, int n_inputs,
                       Batch *out) {
  const Val *join = &child;
  const std::vector<Val> *proj = nullptr;
  std::vector<Val> proj_store;
  std::vector<Val> preds_on_join;
  std::vector<Val> preds_on_proj;
  std::vector<std::pair<bool, Val>> wraps; /* outer-first: true=project */
  while (join->kind == ValKind::CALL &&
         (join->name == "project" || join->name == "filter") && join->args.size() >= 2) {
    if (join->name == "project") {
      wraps.emplace_back(true, join->args[0]);
      join = &join->args[1];
    } else {
      wraps.emplace_back(false, join->args[0]);
      join = &join->args[1];
    }
  }
  for (int i = static_cast<int>(wraps.size()) - 1; i >= 0; --i) {
    if (wraps[static_cast<size_t>(i)].first) {
      if (wraps[static_cast<size_t>(i)].second.kind == ValKind::CALL) {
        proj_store = wraps[static_cast<size_t>(i)].second.args;
        proj = &proj_store;
      }
    } else if (proj) {
      preds_on_proj.push_back(wraps[static_cast<size_t>(i)].second);
    } else {
      preds_on_join.push_back(wraps[static_cast<size_t>(i)].second);
    }
  }
  if (join->kind != ValKind::CALL || join->name != "hashjoin") return false;
  Batch probe;
  std::vector<JoinLevel> levels;
  const int pidx = leftmost_file_scan(join->args[2]);
  const int nsplits = file_n_splits(pidx);
  if (pidx >= 0 && nsplits > 1) {
    Batch first = eval_plan_probe_split(join->args[2], pidx, 0, inputs, n_inputs);
    if (!flatten_joins(*join, &probe, &levels, inputs, n_inputs, &first) || levels.empty()) {
      return false;
    }
  } else if (!flatten_joins(*join, &probe, &levels, inputs, n_inputs, nullptr) ||
             levels.empty()) {
    return false;
  }
  std::fprintf(stderr, "nativesql: fused hashagg join levels=%zu probe_rows=%d splits=%d\n",
               levels.size(), probe.n_rows, nsplits > 1 ? nsplits : 1);
  size_t ht_hint = static_cast<size_t>(probe.n_rows);
  if (pidx >= 0 && nsplits > 1) ht_hint *= static_cast<size_t>(nsplits);
  OpenHash ht(ht_hint + 8);
  std::vector<int> first_probe;
  std::vector<std::vector<int>> first_br;
  std::vector<std::vector<EvalNum>> passthrough;
  std::vector<std::vector<Acc>> accs;
  std::vector<int> br(levels.size(), 0);
  uint64_t hits = 0;
  JoinCtx cx;
  cx.probe = &probe;
  cx.levels = &levels;
  cx.br = &br;

  std::function<void(size_t)> rec = [&](size_t level) {
    if (level == levels.size()) {
      if (++hits > 50000000ull) throw std::runtime_error("hashjoin output cap");
      for (const auto &pred : preds_on_join) {
        if (!virt_eval_pred(pred, cx)) return;
      }
      for (const auto &pred : preds_on_proj) {
        if (pred.kind == ValKind::CALL && pred.name.size() == 2 &&
            (pred.name == "eq" || pred.name == "ne" || pred.name == "gt" ||
             pred.name == "ge" || pred.name == "lt" || pred.name == "le")) {
          EvalNum l = proj_eval(pred.args[0], proj, cx);
          EvalNum r = proj_eval(pred.args[1], proj, cx);
          const bool f = l.is_f || r.is_f;
          bool ok = true;
          if (pred.name == "eq") ok = f ? l.as_f() == r.as_f() : l.as_i() == r.as_i();
          else if (pred.name == "ne") ok = f ? l.as_f() != r.as_f() : l.as_i() != r.as_i();
          else if (pred.name == "gt") ok = f ? l.as_f() > r.as_f() : l.as_i() > r.as_i();
          else if (pred.name == "ge") ok = f ? l.as_f() >= r.as_f() : l.as_i() >= r.as_i();
          else if (pred.name == "lt") ok = f ? l.as_f() < r.as_f() : l.as_i() < r.as_i();
          else if (pred.name == "le") ok = f ? l.as_f() <= r.as_f() : l.as_i() <= r.as_i();
          if (!ok) return;
        } else if (!virt_eval_pred(pred, cx)) {
          return;
        }
      }
      const uint64_t k = virt_row_key(keys, proj, cx);
      int g = ht.find(k);
      if (g < 0) {
        g = static_cast<int>(first_probe.size());
        if (g > 2000000) throw std::runtime_error("hashagg groups cap");
        ht.insert(k, g);
        first_probe.push_back(cx.pr);
        first_br.push_back(br);
        accs.emplace_back(aggs.size());
        passthrough.emplace_back(aggs.size());
        for (size_t a = 0; a < aggs.size(); ++a) {
          if (aggs[a].kind != ValKind::CALL) {
            passthrough.back()[a] = proj_eval(aggs[a], proj, cx);
          }
        }
      }
      for (size_t a = 0; a < aggs.size(); ++a) {
        acc_update(accs[static_cast<size_t>(g)][a], aggs[a], proj, cx);
      }
      return;
    }
    const JoinLevel &lv = levels[level];
    const int64_t kv = virt_eval_num(lv.lk, cx).as_i();
    const int g = lv.ht->find(mix(static_cast<uint64_t>(kv)));
    if (g < 0) return;
    for (int r : lv.buckets[static_cast<size_t>(g)]) {
      if (eval_num(lv.rk, lv.build, r).as_i() != kv) continue;
      br[level] = r;
      rec(level + 1);
    }
  };

  auto consume = [&]() {
    for (int p = 0; p < probe.n_rows; ++p) {
      cx.pr = p;
      rec(0);
    }
  };
  consume();
  if (pidx >= 0 && nsplits > 1) {
    for (int s = 1; s < nsplits; ++s) {
      probe = eval_plan_probe_split(join->args[2], pidx, s, inputs, n_inputs);
      cx.probe = &probe;
      consume();
    }
  }

  const int ng = static_cast<int>(first_probe.size());
  out->n_rows = ng;
  JoinCtx sample = cx;
  sample.pr = ng ? first_probe[0] : 0;
  if (ng) sample.br = &first_br[0];
  for (size_t a = 0; a < aggs.size(); ++a) {
    const Val &fn = aggs[a];
    NsType t = NS_I64;
    if (fn.kind == ValKind::CALL && fn.name == "avg") {
      t = NS_F64;
    } else if (fn.kind != ValKind::CALL && ng) {
      if (passthrough[0][a].is_f) t = NS_F64;
    } else if (fn.kind == ValKind::CALL && !fn.args.empty()) {
      if (proj_eval(fn.args[0], proj, sample).is_f) t = NS_F64;
    }
    out->add_col(t, ng);
    for (int g = 0; g < ng; ++g) {
      JoinCtx z = cx;
      z.pr = first_probe[static_cast<size_t>(g)];
      z.br = &first_br[static_cast<size_t>(g)];
      if (fn.kind != ValKind::CALL) {
        EvalNum n = passthrough[static_cast<size_t>(g)][a];
        if (t == NS_F64) out->f64[a][g] = n.as_f();
        else if (t == NS_BOOL) out->b[a][g] = n.as_i() ? 1 : 0;
        else out->i64[a][g] = n.as_i();
        continue;
      }
      const Acc &ac = accs[static_cast<size_t>(g)][a];
      if (fn.name == "count") out->i64[a][g] = ac.cnt;
      else if (fn.name == "sum") {
        if (t == NS_F64) out->f64[a][g] = ac.is_f ? ac.fsum : static_cast<double>(ac.isum);
        else out->i64[a][g] = ac.isum;
      } else if (fn.name == "avg") {
        out->f64[a][g] =
            ac.cnt ? (ac.is_f ? ac.fsum : static_cast<double>(ac.isum)) / ac.cnt : NAN;
      } else if (fn.name == "min") {
        if (t == NS_F64) out->f64[a][g] = ac.fmin;
        else out->i64[a][g] = ac.imin;
      } else if (fn.name == "max") {
        if (t == NS_F64) out->f64[a][g] = ac.fmax;
        else out->i64[a][g] = ac.imax;
      }
    }
  }
  return true;
}

Batch eval_plan(const Val &node, const NsBatch *inputs, int n_inputs) {
  if (node.kind != ValKind::CALL) throw std::runtime_error("plan must be a call");
  const auto &n = node.name;
  if (n == "scan") {
    const int idx = static_cast<int>(node.args[0].i);
    if (idx < 0 || idx >= n_inputs) throw std::runtime_error("scan oob");
    if (g_file_scans && g_file_scans[idx].n_splits > 0) {
      NsFileScan sc = attach_preds(idx);
      NsBatch raw{};
      if (ns_parquet_read(&sc, &raw) != 0) {
        throw std::runtime_error("parquet scan failed");
      }
      std::fprintf(stderr, "nativesql: from_c scan=%d rows=%d cols=%d\n",
                   idx, raw.n_rows, raw.n_cols);
      std::fflush(stderr);
      Batch b = from_c(raw);
      ns_batch_free(&raw);
      std::fprintf(stderr, "nativesql: from_c done scan=%d\n", idx);
      std::fflush(stderr);
      return b;
    }
    return from_c(inputs[idx]);
  }
  if (n == "range") {
    return make_range(node.args[0].i, node.args[1].i, node.args[2].i);
  }
  if (n == "filter") {
    PredScope scope(direct_file_scan(node.args[1]), node.args[0]);
    Batch child = eval_plan(node.args[1], inputs, n_inputs);
    return do_filter(std::move(child), node.args[0]);
  }
  if (n == "project") {
    const Val &child = node.args[1];
    if (child.kind == ValKind::CALL && child.name == "hashjoin") {
      Batch joined;
      if (try_chunked_join(child, &node.args[0].args, inputs, n_inputs, &joined)) {
        return joined;
      }
    }
    Batch childb = eval_plan(child, inputs, n_inputs);
    return project_cols(childb, node.args[0].args);
  }
  if (n == "hashagg") {
    Batch fused;
    if (try_fused_hashagg(node.args[2], node.args[0].args, node.args[1].args, inputs,
                          n_inputs, &fused) ||
        try_fused_scan_hashagg(node.args[2], node.args[0].args, node.args[1].args, inputs,
                               n_inputs, &fused)) {
      return fused;
    }
    Batch child = eval_plan(node.args[2], inputs, n_inputs);
    return do_hashagg(child, node.args[0].args, node.args[1].args);
  }
  if (n == "segagg") {
    if (node.args.size() < 4) throw std::runtime_error("segagg arity");
    Batch child = eval_plan(node.args[3], inputs, n_inputs);
    return do_segagg(child, node.args[0], node.args[1].args, node.args[2].args);
  }
  if (n == "hashjoin") {
    Batch joined;
    if (try_chunked_join(node, nullptr, inputs, n_inputs, &joined)) {
      return joined;
    }
    Batch left = eval_plan(node.args[2], inputs, n_inputs);
    Batch right = eval_plan(node.args[3], inputs, n_inputs);
    return do_join(left, right, node.args[0], node.args[1]);
  }
  if (n == "hashjoinidx") {
    Batch left = eval_plan(node.args[2], inputs, n_inputs);
    Batch right = eval_plan(node.args[3], inputs, n_inputs);
    return do_join_idx(left, right, node.args[0], node.args[1]);
  }
  if (n == "hashsemi") {
    Batch left = eval_plan(node.args[2], inputs, n_inputs);
    Batch right = eval_plan(node.args[3], inputs, n_inputs);
    return do_semi(left, right, node.args[0], node.args[1]);
  }
  if (n == "union") {
    Batch left = eval_plan(node.args[0], inputs, n_inputs);
    Batch right = eval_plan(node.args[1], inputs, n_inputs);
    return do_union(left, right);
  }
  if (n == "sort") {
    if (node.args.empty()) throw std::runtime_error("sort missing child");
    std::vector<Val> keys;
    if (node.args[0].kind == ValKind::CALL && node.args[0].name == "list") {
      keys = node.args[0].args;
    } else {
      for (size_t i = 0; i + 1 < node.args.size(); ++i) keys.push_back(node.args[i]);
    }
    Batch child = eval_plan(node.args.back(), inputs, n_inputs);
    return do_sort(child, keys);
  }
  throw std::runtime_error("unknown op " + n);
}

} // namespace

void ns_batch_free(NsBatch *b) {
  if (!b) return;
  if (b->cols) {
    for (int i = 0; i < b->n_cols; ++i) std::free(b->cols[i].data);
    std::free(b->cols);
  }
  b->cols = nullptr;
  b->n_cols = 0;
  b->n_rows = 0;
}

int ns_execute_scan(
    const char *plan_ir,
    const NsBatch *inputs,
    const NsFileScan *scans,
    int n_inputs,
    NsBatch *out) {
  g_file_scans = scans;
  try {
    std::fprintf(stderr, "nativesql: exec ir=%s n=%d\n", plan_ir ? plan_ir : "", n_inputs);
    if (g_file_scans) {
      for (int i = 0; i < n_inputs; ++i) {
        std::fprintf(stderr, "nativesql: leaf %d splits=%d cols=%d\n",
                     i, g_file_scans[i].n_splits, g_file_scans[i].n_cols);
      }
    }
    std::fflush(stderr);
    Tok t(plan_ir);
    Val plan = parse_val(t);
    Batch r = eval_plan(plan, inputs, n_inputs);
    out->n_cols = r.n_cols();
    out->n_rows = r.n_rows;
    out->cols = static_cast<NsCol *>(std::calloc(static_cast<size_t>(out->n_cols), sizeof(NsCol)));
    for (int c = 0; c < r.n_cols(); ++c) {
      out->cols[c].type = r.types[c];
      out->cols[c].n_rows = r.n_rows;
      const int n = r.n_rows < 0 ? 0 : r.n_rows;
      if (r.types[c] == NS_I32 || r.types[c] == NS_I64) {
        int64_t *p = static_cast<int64_t *>(std::malloc(sizeof(int64_t) * static_cast<size_t>(n)));
        const size_t avail = r.i64[c].size();
        const size_t ncopy = avail < static_cast<size_t>(n) ? avail : static_cast<size_t>(n);
        if (ncopy && r.i64[c].data() != nullptr && p != nullptr) {
          std::memcpy(p, r.i64[c].data(), sizeof(int64_t) * ncopy);
        }
        out->cols[c].type = NS_I64;
        out->cols[c].data = p;
      } else if (r.types[c] == NS_F64) {
        double *p = static_cast<double *>(std::malloc(sizeof(double) * static_cast<size_t>(n)));
        const size_t avail = r.f64[c].size();
        const size_t ncopy = avail < static_cast<size_t>(n) ? avail : static_cast<size_t>(n);
        if (ncopy && r.f64[c].data() != nullptr && p != nullptr) {
          std::memcpy(p, r.f64[c].data(), sizeof(double) * ncopy);
        }
        out->cols[c].data = p;
      } else {
        uint8_t *p = static_cast<uint8_t *>(std::malloc(static_cast<size_t>(n)));
        const size_t avail = r.b[c].size();
        const size_t ncopy = avail < static_cast<size_t>(n) ? avail : static_cast<size_t>(n);
        if (ncopy && r.b[c].data() != nullptr && p != nullptr) {
          std::memcpy(p, r.b[c].data(), ncopy);
        }
        out->cols[c].data = p;
      }
    }
    g_file_scans = nullptr;
    return 0;
  } catch (...) {
    g_file_scans = nullptr;
    return -1;
  }
}

int ns_execute(const char *plan_ir, const NsBatch *inputs, int n_inputs, NsBatch *out) {
  return ns_execute_scan(plan_ir, inputs, nullptr, n_inputs, out);
}
