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

#include <cctype>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <stdexcept>

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
    if (types[col] == NS_F64) return static_cast<int64_t>(f64[col][row]);
    if (types[col] == NS_BOOL) return b[col][row] ? 1 : 0;
    return i64[col][row];
  }
  double get_f(int col, int row) const {
    if (types[col] == NS_F64) return f64[col][row];
    if (types[col] == NS_BOOL) return b[col][row] ? 1.0 : 0.0;
    return static_cast<double>(i64[col][row]);
  }
  bool get_b(int col, int row) const { return get_i(col, row) != 0; }
};

Batch from_c(const NsBatch &in) {
  Batch b;
  b.n_rows = in.n_rows;
  for (int c = 0; c < in.n_cols; ++c) {
    const NsCol &col = in.cols[c];
    b.add_col(col.type, in.n_rows);
    if (col.type == NS_I32) {
      const int32_t *p = static_cast<const int32_t *>(col.data);
      for (int r = 0; r < in.n_rows; ++r) b.i64[c][r] = p[r];
    } else if (col.type == NS_I64) {
      const int64_t *p = static_cast<const int64_t *>(col.data);
      for (int r = 0; r < in.n_rows; ++r) b.i64[c][r] = p[r];
    } else if (col.type == NS_F64) {
      const double *p = static_cast<const double *>(col.data);
      for (int r = 0; r < in.n_rows; ++r) b.f64[c][r] = p[r];
    } else {
      const uint8_t *p = static_cast<const uint8_t *>(col.data);
      for (int r = 0; r < in.n_rows; ++r) b.b[c][r] = p[r] ? 1 : 0;
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

EvalNum eval_num(const Val &v, const Batch &b, int row) {
  if (v.kind == ValKind::I64) return {false, v.i, 0};
  if (v.kind == ValKind::F64) return {true, 0, v.d};
  if (v.kind == ValKind::BOOL) return {false, v.b ? 1 : 0, 0};
  if (v.kind == ValKind::COL) {
    const int c = static_cast<int>(v.i);
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
  }
  throw std::runtime_error("cannot eval numeric");
}

bool eval_pred(const Val &v, const Batch &b, int row) {
  if (v.kind == ValKind::BOOL) return v.b;
  if (v.kind == ValKind::COL) return b.get_b(static_cast<int>(v.i), row);
  if (v.kind != ValKind::CALL) return eval_num(v, b, row).as_i() != 0;
  const auto &n = v.name;
  if (n == "and") return eval_pred(v.args[0], b, row) && eval_pred(v.args[1], b, row);
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
    else if (e.kind == ValKind::COL) t = in.types[static_cast<int>(e.i)];
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

Batch do_filter(const Batch &in, const Val &pred) {
  std::vector<int> keep;
  keep.reserve(in.n_rows);
  for (int r = 0; r < in.n_rows; ++r) {
    if (eval_pred(pred, in, r)) keep.push_back(r);
  }
  Batch out;
  out.n_rows = static_cast<int>(keep.size());
  for (int c = 0; c < in.n_cols(); ++c) {
    out.add_col(in.types[c], out.n_rows);
    for (int i = 0; i < out.n_rows; ++i) {
      const int r = keep[i];
      if (in.types[c] == NS_F64) out.f64[c][i] = in.f64[c][r];
      else if (in.types[c] == NS_BOOL) out.b[c][i] = in.b[c][r];
      else out.i64[c][i] = in.i64[c][r];
    }
  }
  return out;
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

uint64_t row_key(const Batch &b, const std::vector<Val> &keys, int row) {
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
  for (int r = 0; r < in.n_rows; ++r) {
    const uint64_t k = row_key(in, keys, r);
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
        if (fn.args.empty()) ac.cnt += 1;
        else ac.cnt += 1;
        continue;
      }
      EvalNum x = eval_num(fn.args[0], in, r);
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

Batch do_join(const Batch &left, const Batch &right, const Val &lk, const Val &rk) {
  OpenHash ht(static_cast<size_t>(right.n_rows) + 8);
  std::vector<std::vector<int>> buckets(right.n_rows + 1);
  int next = 0;
  for (int r = 0; r < right.n_rows; ++r) {
    const uint64_t k = mix(static_cast<uint64_t>(eval_num(rk, right, r).as_i()));
    int g = ht.find(k);
    if (g < 0) {
      g = next++;
      ht.insert(k, g);
    }
    buckets[g].push_back(r);
  }
  std::vector<int> lr, rr;
  for (int l = 0; l < left.n_rows; ++l) {
    const uint64_t k = mix(static_cast<uint64_t>(eval_num(lk, left, l).as_i()));
    const int g = ht.find(k);
    if (g < 0) continue;
    for (int r : buckets[g]) {
      if (eval_num(lk, left, l).as_i() == eval_num(rk, right, r).as_i()) {
        lr.push_back(l);
        rr.push_back(r);
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

Batch eval_plan(const Val &node, const NsBatch *inputs, int n_inputs) {
  if (node.kind != ValKind::CALL) throw std::runtime_error("plan must be a call");
  const auto &n = node.name;
  if (n == "scan") {
    const int idx = static_cast<int>(node.args[0].i);
    if (idx < 0 || idx >= n_inputs) throw std::runtime_error("scan oob");
    return from_c(inputs[idx]);
  }
  if (n == "range") {
    return make_range(node.args[0].i, node.args[1].i, node.args[2].i);
  }
  if (n == "filter") {
    Batch child = eval_plan(node.args[1], inputs, n_inputs);
    return do_filter(child, node.args[0]);
  }
  if (n == "project") {
    Batch child = eval_plan(node.args[1], inputs, n_inputs);
    return project_cols(child, node.args[0].args);
  }
  if (n == "hashagg") {
    Batch child = eval_plan(node.args[2], inputs, n_inputs);
    return do_hashagg(child, node.args[0].args, node.args[1].args);
  }
  if (n == "hashjoin") {
    Batch left = eval_plan(node.args[2], inputs, n_inputs);
    Batch right = eval_plan(node.args[3], inputs, n_inputs);
    return do_join(left, right, node.args[0], node.args[1]);
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

int ns_execute(const char *plan_ir, const NsBatch *inputs, int n_inputs, NsBatch *out) {
  try {
    Tok t(plan_ir);
    Val plan = parse_val(t);
    Batch r = eval_plan(plan, inputs, n_inputs);
    out->n_cols = r.n_cols();
    out->n_rows = r.n_rows;
    out->cols = static_cast<NsCol *>(std::calloc(static_cast<size_t>(out->n_cols), sizeof(NsCol)));
    for (int c = 0; c < r.n_cols(); ++c) {
      out->cols[c].type = r.types[c];
      out->cols[c].n_rows = r.n_rows;
      if (r.types[c] == NS_I32 || r.types[c] == NS_I64) {
        int64_t *p = static_cast<int64_t *>(std::malloc(sizeof(int64_t) * static_cast<size_t>(r.n_rows)));
        if (r.n_rows) std::memcpy(p, r.i64[c].data(), sizeof(int64_t) * static_cast<size_t>(r.n_rows));
        out->cols[c].type = NS_I64;
        out->cols[c].data = p;
      } else if (r.types[c] == NS_F64) {
        double *p = static_cast<double *>(std::malloc(sizeof(double) * static_cast<size_t>(r.n_rows)));
        if (r.n_rows) std::memcpy(p, r.f64[c].data(), sizeof(double) * static_cast<size_t>(r.n_rows));
        out->cols[c].data = p;
      } else {
        uint8_t *p = static_cast<uint8_t *>(std::malloc(static_cast<size_t>(r.n_rows)));
        if (r.n_rows) std::memcpy(p, r.b[c].data(), static_cast<size_t>(r.n_rows));
        out->cols[c].data = p;
      }
    }
    return 0;
  } catch (...) {
    return -1;
  }
}
