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

#ifdef VEGAM_HAS_VELOX

#include "vegam_engine.h"
#include "vegam_hadoop_file.h"
#include "vegam_pipeline.h"
#include "vegam_scheduler.h"

#include <atomic>
#include <cmath>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "velox/common/memory/Memory.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/exec/AggregateFunctionRegistry.h"
#include "velox/exec/Split.h"
#include "velox/exec/Task.h"
#include "velox/expression/RegisterSpecialForm.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/sparksql/aggregates/Register.h"
#include "velox/functions/sparksql/registration/Register.h"
#include "velox/type/Filter.h"
#include "velox/vector/arrow/Abi.h"
#include "velox/vector/arrow/Bridge.h"

/*
 * Velox execution of one Vegam stage: Hive/Parquet TableScan over the task's
 * file ranges, broadcast hash joins, and a Spark-semantics partial
 * aggregation whose output matches Spark's partial buffer layout. Results
 * stream out as Arrow C Data batches.
 *
 * vegam_velox_start returns nullptr for any stage shape this path does not
 * cover; the caller then runs the fused C++ pipeline.
 */

using namespace facebook::velox;
namespace hive = facebook::velox::connector::hive;

namespace {

constexpr const char* kHiveId = "vegam-hive";
// Spark has no count in velox/functions/sparksql; take Presto's under a prefix
// so it never shadows a Spark-semantics function.
constexpr const char* kPresto = "presto_";

void ensure_velox() {
  static std::once_flag once;
  std::call_once(once, [] {
    memory::MemoryManager::initialize(memory::MemoryManager::Options{});
    exec::registerFunctionCallToSpecialForms();
    functions::sparksql::registerFunctions("");
    functions::aggregate::sparksql::registerAggregateFunctions("");
    aggregate::prestosql::registerAllAggregateFunctions(kPresto);
    parquet::registerParquetReaderFactory();
    vegam_register_hadoop_fs();
    // Spark resolves file columns by name; Velox defaults to Hive's by-index.
    auto cfg = std::make_shared<config::ConfigBase>(
        std::unordered_map<std::string, std::string>{
            {"hive.parquet.use-column-names", "true"},
            {"orc.use-column-names", "true"}});
    connector::registerConnector(
        hive::HiveConnectorFactory().newConnector(kHiveId, cfg));
    std::cerr << "vegam: velox ready (hive parquet scan, sparksql functions)"
              << std::endl;
  });
}

struct Unsupported : std::runtime_error {
  explicit Unsupported(const std::string& why) : std::runtime_error(why) {}
};

TypePtr spark_type(const std::string& s) {
  if (s == "int") return INTEGER();
  if (s == "bigint") return BIGINT();
  if (s == "smallint") return SMALLINT();
  if (s == "tinyint") return TINYINT();
  if (s == "double") return DOUBLE();
  if (s == "float") return REAL();
  if (s == "boolean") return BOOLEAN();
  if (s == "string") return VARCHAR();
  if (s == "date") return DATE();
  if (s == "binary") return VARBINARY();
  if (s.rfind("char(", 0) == 0 || s.rfind("varchar(", 0) == 0) return VARCHAR();
  if (s.rfind("decimal(", 0) == 0) {
    int p = 0;
    int sc = 0;
    if (std::sscanf(s.c_str(), "decimal(%d,%d)", &p, &sc) == 2) {
      return DECIMAL(p, sc);
    }
  }
  throw Unsupported("type " + s);
}

bool is_integral(const TypePtr& t) {
  switch (t->kind()) {
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
      return !t->isDecimal();
    default:
      return false;
  }
}

/** Columns of one scan with their Spark types. */
struct ScanCols {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  std::unordered_set<std::string> part_keys;

  int index(const std::string& n) const {
    for (size_t i = 0; i < names.size(); i++) {
      if (names[i] == n) {
        return static_cast<int>(i);
      }
    }
    return -1;
  }

  bool has(const std::string& n) const {
    return index(n) >= 0;
  }

  TypePtr type(const std::string& n) const {
    int i = index(n);
    if (i < 0) {
      throw Unsupported("unknown column " + n);
    }
    return types[i];
  }
};

ScanCols scan_cols(const VegamScan& s) {
  if (s.types.size() != s.columns.size() || s.columns.empty()) {
    throw Unsupported("scan without types");
  }
  ScanCols c;
  for (size_t i = 0; i < s.columns.size(); i++) {
    if (c.has(s.columns[i])) {
      continue;
    }
    c.names.push_back(s.columns[i]);
    c.types.push_back(spark_type(s.types[i]));
  }
  for (const auto& f : s.files) {
    for (const auto& kv : f.parts) {
      c.part_keys.insert(kv.first);
    }
  }
  return c;
}

core::FieldAccessTypedExprPtr field(const TypePtr& t, const std::string& n) {
  return std::make_shared<core::FieldAccessTypedExpr>(t, n);
}

core::TypedExprPtr call(const std::string& fn, std::vector<core::TypedExprPtr> args,
                        const TypePtr& type = BOOLEAN()) {
  return std::make_shared<core::CallTypedExpr>(type, std::move(args), fn);
}

core::TypedExprPtr conj(core::TypedExprPtr a, core::TypedExprPtr b) {
  if (a == nullptr) {
    return b;
  }
  return call("and", {std::move(a), std::move(b)});
}

double filter_number(const VegamFilter& f) {
  return std::isnan(f.dvalue) ? static_cast<double>(f.value) : f.dvalue;
}

/** Literal of the filter value in the column type, or nullptr if not exact. */
core::TypedExprPtr exact_literal(const TypePtr& t, const VegamFilter& f) {
  if (t->kind() == TypeKind::VARCHAR) {
    if (!f.is_string) {
      return nullptr;
    }
    return std::make_shared<core::ConstantTypedExpr>(VARCHAR(), variant(f.str));
  }
  if (f.is_string) {
    return nullptr;
  }
  const double v = filter_number(f);
  if (t->isShortDecimal() || t->isLongDecimal()) {
    const int scale = getDecimalPrecisionScale(*t).second;
    const double scaled = v * std::pow(10.0, scale);
    const double rounded = std::llround(scaled);
    if (std::fabs(scaled - rounded) > 1e-6) {
      return nullptr;
    }
    const auto unscaled = static_cast<int64_t>(rounded);
    if (t->isShortDecimal()) {
      return std::make_shared<core::ConstantTypedExpr>(t, variant(unscaled));
    }
    return std::make_shared<core::ConstantTypedExpr>(
        t, variant(static_cast<int128_t>(unscaled)));
  }
  if (is_integral(t) || t->kind() == TypeKind::INTEGER) {
    // DATE is INTEGER days in Velox and in the plan literal.
    if (!std::isnan(f.dvalue) && std::floor(f.dvalue) != f.dvalue) {
      return nullptr;
    }
    const auto iv = static_cast<int64_t>(v);
    switch (t->kind()) {
      case TypeKind::TINYINT:
        return std::make_shared<core::ConstantTypedExpr>(t, variant(static_cast<int8_t>(iv)));
      case TypeKind::SMALLINT:
        return std::make_shared<core::ConstantTypedExpr>(t, variant(static_cast<int16_t>(iv)));
      case TypeKind::INTEGER:
        return std::make_shared<core::ConstantTypedExpr>(t, variant(static_cast<int32_t>(iv)));
      default:
        return std::make_shared<core::ConstantTypedExpr>(t, variant(iv));
    }
  }
  if (t->kind() == TypeKind::DOUBLE) {
    return std::make_shared<core::ConstantTypedExpr>(t, variant(v));
  }
  if (t->kind() == TypeKind::REAL) {
    return std::make_shared<core::ConstantTypedExpr>(t, variant(static_cast<float>(v)));
  }
  return nullptr;
}

const char* cmp_fn(int op) {
  switch (op) {
    case 1: return "greaterthan";
    case 2: return "greaterthanorequal";
    case 3: return "lessthan";
    case 4: return "lessthanorequal";
    case 5: return "equalto";
    case 6: return "equalto";  // wrapped in not()
    default: return nullptr;
  }
}

core::TypedExprPtr predicate(const ScanCols& cols, const VegamFilter& f) {
  const char* fn = cmp_fn(f.op);
  if (fn == nullptr) {
    throw Unsupported("filter op " + std::to_string(f.op));
  }
  TypePtr t = cols.type(f.col);
  core::TypedExprPtr left = field(t, f.col);
  core::TypedExprPtr lit = exact_literal(t, f);
  if (lit == nullptr) {
    if (f.is_string || !(t->isDecimal() || is_integral(t) || t->kind() == TypeKind::DOUBLE ||
                         t->kind() == TypeKind::REAL)) {
      throw Unsupported("filter literal on " + f.col);
    }
    left = std::make_shared<core::CastTypedExpr>(DOUBLE(), left, false);
    lit = std::make_shared<core::ConstantTypedExpr>(DOUBLE(), variant(filter_number(f)));
  }
  auto p = call(fn, {left, lit});
  return f.op == 6 ? call("not", {p}) : p;
}

/** Subfield filter for integer / date ranges and string equality. */
common::FilterPtr subfield_filter(const TypePtr& t, const VegamFilter& f) {
  if (t->kind() == TypeKind::VARCHAR && f.is_string && f.op == 5) {
    return std::make_shared<common::BytesValues>(std::vector<std::string>{f.str}, false);
  }
  if (f.is_string || !is_integral(t) || f.op == 6) {
    return nullptr;
  }
  if (!std::isnan(f.dvalue) && std::floor(f.dvalue) != f.dvalue) {
    return nullptr;
  }
  const auto v = static_cast<int64_t>(filter_number(f));
  constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
  constexpr int64_t kMax = std::numeric_limits<int64_t>::max();
  switch (f.op) {
    case 1: return std::make_shared<common::BigintRange>(v == kMax ? kMax : v + 1, kMax, false);
    case 2: return std::make_shared<common::BigintRange>(v, kMax, false);
    case 3: return std::make_shared<common::BigintRange>(kMin, v == kMin ? kMin : v - 1, false);
    case 4: return std::make_shared<common::BigintRange>(kMin, v, false);
    case 5: return std::make_shared<common::BigintRange>(v, v, false);
    default: return nullptr;
  }
}

struct ScanSplits {
  core::PlanNodeId id;
  std::vector<std::shared_ptr<connector::ConnectorSplit>> splits;
};

class StageBuilder {
 public:
  std::vector<ScanSplits> scans;

  core::PlanNodePtr scan(
      const VegamScan& s,
      const ScanCols& cols,
      const std::vector<VegamFilter>& filters,
      std::unordered_set<std::string> out,
      bool honor_ranges) {
    // Filters on partition keys run above the scan (Spark already pruned the
    // static ones); the key must then be read.
    std::vector<VegamFilter> part_filters;
    common::SubfieldFilters subfields;
    core::TypedExprPtr remaining;
    for (const auto& f : filters) {
      if (cols.part_keys.count(f.col) > 0) {
        part_filters.push_back(f);
        out.insert(f.col);
        continue;
      }
      TypePtr t = cols.type(f.col);
      if (auto sf = subfield_filter(t, f)) {
        common::Subfield key(f.col);
        auto it = subfields.find(key);
        if (it == subfields.end()) {
          subfields.emplace(std::move(key), std::move(sf));
        } else {
          it->second = it->second->mergeWith(sf.get());
        }
      } else {
        remaining = conj(remaining, predicate(cols, f));
      }
    }

    std::vector<std::string> data_names;
    std::vector<TypePtr> data_types;
    std::vector<std::string> out_names;
    std::vector<TypePtr> out_types;
    connector::ColumnHandleMap assignments;
    for (size_t i = 0; i < cols.names.size(); i++) {
      const auto& n = cols.names[i];
      const bool part = cols.part_keys.count(n) > 0;
      if (!part) {
        data_names.push_back(n);
        data_types.push_back(cols.types[i]);
      }
      if (out.count(n) == 0) {
        continue;
      }
      out_names.push_back(n);
      out_types.push_back(cols.types[i]);
      assignments[n] = std::make_shared<hive::HiveColumnHandle>(
          n,
          part ? hive::HiveColumnHandle::ColumnType::kPartitionKey
               : hive::HiveColumnHandle::ColumnType::kRegular,
          cols.types[i],
          cols.types[i]);
    }
    auto handle = std::make_shared<hive::HiveTableHandle>(
        kHiveId, "vegam", std::move(subfields), remaining,
        ROW(std::move(data_names), std::move(data_types)));
    const auto id = next_id();
    core::PlanNodePtr node = std::make_shared<core::TableScanNode>(
        id, ROW(std::move(out_names), std::move(out_types)), handle, assignments);

    ScanSplits ss{id, {}};
    for (const auto& f : s.files) {
      std::unordered_map<std::string, std::optional<std::string>> parts;
      for (const auto& k : cols.part_keys) {
        parts[k] = std::nullopt;
      }
      for (const auto& kv : f.parts) {
        parts[kv.first] = kv.second;
      }
      const uint64_t start = honor_ranges ? static_cast<uint64_t>(f.start) : 0;
      const uint64_t len = (!honor_ranges || f.length < 0)
          ? std::numeric_limits<uint64_t>::max()
          : static_cast<uint64_t>(f.length);
      ss.splits.push_back(std::make_shared<hive::HiveConnectorSplit>(
          kHiveId, f.path, dwio::common::FileFormat::PARQUET, start, len, parts));
    }
    scans.push_back(std::move(ss));

    core::TypedExprPtr pf;
    for (const auto& f : part_filters) {
      pf = conj(pf, predicate(cols, f));
    }
    if (pf != nullptr) {
      node = std::make_shared<core::FilterNode>(next_id(), pf, node);
    }
    return node;
  }

  std::string next_id() {
    return std::to_string(id_++);
  }

 private:
  int id_ = 0;
};

core::JoinType join_type(int t) {
  switch (t) {
    case 1: return core::JoinType::kInner;
    case 2: return core::JoinType::kLeft;
    case 3: return core::JoinType::kLeftSemiFilter;
    case 4: return core::JoinType::kAnti;
    default: throw Unsupported("join type " + std::to_string(t));
  }
}

bool filter_pushable_to_build(int join) {
  return join == 1 || join == 3;
}

const char* agg_fn(int kind) {
  switch (kind) {
    case 1: return "sum";
    case 2:
    case 3: return "presto_count";
    case 4: return "min";
    case 5: return "max";
    case 6: return "avg";
    default: return nullptr;
  }
}

/** Plan + splits for one stage, or throws Unsupported. */
struct BuiltStage {
  core::PlanNodePtr root;
  std::vector<ScanSplits> scans;
};

BuiltStage build_stage(const VegamDecoded& plan) {
  if (plan.kind != 3) {
    throw Unsupported("plan kind " + std::to_string(plan.kind));
  }
  if (plan.has_window || plan.has_expand) {
    throw Unsupported("window/expand");
  }
  if (plan.complete) {
    throw Unsupported("complete aggregation");
  }
  if (plan.groups.empty() && plan.aggs.empty()) {
    throw Unsupported("no aggregation");
  }
  for (const auto& b : plan.builds) {
    if (!b.broadcast) {
      throw Unsupported("shuffle-side join build");
    }
  }

  const ScanCols probe_cols = scan_cols(plan.probe);
  std::vector<ScanCols> build_cols;
  for (const auto& b : plan.builds) {
    build_cols.push_back(scan_cols(b.scan));
  }

  // Assign each stage filter to the scan that owns its column when that is
  // equivalent to filtering after the joins; the rest run after the joins.
  std::vector<VegamFilter> probe_filters;
  std::vector<std::vector<VegamFilter>> build_filters(plan.builds.size());
  std::vector<VegamFilter> post_filters;
  for (size_t i = 0; i < plan.builds.size(); i++) {
    build_filters[i] = plan.builds[i].filters;
  }
  for (const auto& f : plan.filters) {
    if (probe_cols.has(f.col)) {
      probe_filters.push_back(f);
      continue;
    }
    bool placed = false;
    for (size_t i = 0; i < plan.builds.size() && !placed; i++) {
      if (build_cols[i].has(f.col)) {
        if (filter_pushable_to_build(plan.builds[i].join_type)) {
          build_filters[i].push_back(f);
        } else {
          post_filters.push_back(f);
        }
        placed = true;
      }
    }
    if (!placed) {
      throw Unsupported("filter column " + f.col + " not on any scan");
    }
  }

  // Columns needed after join i: groups, agg inputs, post filters, and the
  // probe keys of later joins.
  std::unordered_set<std::string> tail;
  for (const auto& g : plan.groups) tail.insert(g);
  for (const auto& a : plan.aggs) {
    if (!a.col.empty()) tail.insert(a.col);
    if (a.input == "?") throw Unsupported("aggregate input expression");
  }
  for (const auto& f : post_filters) tail.insert(f.col);
  std::vector<std::unordered_set<std::string>> needed_after(plan.builds.size());
  {
    std::unordered_set<std::string> acc = tail;
    for (size_t i = plan.builds.size(); i-- > 0;) {
      needed_after[i] = acc;
      for (const auto& k : plan.builds[i].probe_keys) acc.insert(k);
    }
    tail = acc;  // what the probe scan must produce
  }

  StageBuilder sb;
  std::unordered_set<std::string> probe_out;
  for (const auto& n : probe_cols.names) {
    if (tail.count(n) > 0) probe_out.insert(n);
  }
  core::PlanNodePtr node = sb.scan(plan.probe, probe_cols, probe_filters, probe_out, true);

  for (size_t i = 0; i < plan.builds.size(); i++) {
    const auto& b = plan.builds[i];
    const auto& bc = build_cols[i];
    if (b.probe_keys.size() != b.build_keys.size() || b.probe_keys.empty()) {
      throw Unsupported("join keys");
    }
    std::unordered_set<std::string> build_out(b.build_keys.begin(), b.build_keys.end());
    for (const auto& n : bc.names) {
      if (needed_after[i].count(n) > 0) build_out.insert(n);
    }
    core::PlanNodePtr build = sb.scan(b.scan, bc, build_filters[i], build_out, false);

    const auto& left_type = node->outputType();
    const auto& right_type = build->outputType();
    std::vector<core::FieldAccessTypedExprPtr> lk;
    std::vector<core::FieldAccessTypedExprPtr> rk;
    for (size_t k = 0; k < b.probe_keys.size(); k++) {
      auto li = left_type->getChildIdxIfExists(b.probe_keys[k]);
      auto ri = right_type->getChildIdxIfExists(b.build_keys[k]);
      if (!li.has_value() || !ri.has_value()) {
        throw Unsupported("join key not available: " + b.probe_keys[k]);
      }
      const auto& lt = left_type->childAt(*li);
      const auto& rt = right_type->childAt(*ri);
      if (!lt->equivalent(*rt)) {
        throw Unsupported("join key types differ: " + b.probe_keys[k]);
      }
      lk.push_back(field(lt, b.probe_keys[k]));
      rk.push_back(field(rt, b.build_keys[k]));
    }
    const auto jt = join_type(b.join_type);
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    for (size_t c = 0; c < left_type->size(); c++) {
      if (needed_after[i].count(left_type->nameOf(c)) > 0) {
        names.push_back(left_type->nameOf(c));
        types.push_back(left_type->childAt(c));
      }
    }
    if (jt == core::JoinType::kInner || jt == core::JoinType::kLeft) {
      for (size_t c = 0; c < right_type->size(); c++) {
        if (needed_after[i].count(right_type->nameOf(c)) > 0) {
          names.push_back(right_type->nameOf(c));
          types.push_back(right_type->childAt(c));
        }
      }
    }
    if (names.empty()) {
      names.push_back(b.probe_keys[0]);
      types.push_back(lk[0]->type());
    }
    node = std::make_shared<core::HashJoinNode>(
        sb.next_id(), jt, false, lk, rk, nullptr, node, build,
        ROW(std::move(names), std::move(types)));
  }

  if (!post_filters.empty()) {
    ScanCols post;
    const auto& t = node->outputType();
    for (size_t c = 0; c < t->size(); c++) {
      post.names.push_back(t->nameOf(c));
      post.types.push_back(t->childAt(c));
    }
    core::TypedExprPtr pf;
    for (const auto& f : post_filters) pf = conj(pf, predicate(post, f));
    node = std::make_shared<core::FilterNode>(sb.next_id(), pf, node);
  }

  // Pre-projection: group keys as is, one input column per aggregate.
  const auto in_type = node->outputType();
  auto col_type = [&](const std::string& n) {
    auto i = in_type->getChildIdxIfExists(n);
    if (!i.has_value()) throw Unsupported("column not available: " + n);
    return in_type->childAt(*i);
  };
  std::vector<std::string> pnames;
  std::vector<core::TypedExprPtr> pexprs;
  for (const auto& g : plan.groups) {
    pnames.push_back(g);
    pexprs.push_back(field(col_type(g), g));
  }
  std::vector<std::string> agg_in(plan.aggs.size());
  for (size_t i = 0; i < plan.aggs.size(); i++) {
    const auto& a = plan.aggs[i];
    if (a.kind == 3) {
      continue;
    }
    core::TypedExprPtr e = field(col_type(a.col), a.col);
    if (a.input == "unscaled") {
      if (!e->type()->isShortDecimal()) throw Unsupported("unscaled on non short decimal");
      e = call("unscaled_value", {e}, BIGINT());
    } else if (a.input.rfind("cast:", 0) == 0) {
      e = std::make_shared<core::CastTypedExpr>(spark_type(a.input.substr(5)), e, false);
    } else if (!a.input.empty()) {
      throw Unsupported("aggregate input " + a.input);
    }
    agg_in[i] = "_in" + std::to_string(i);
    pnames.push_back(agg_in[i]);
    pexprs.push_back(e);
  }
  node = std::make_shared<core::ProjectNode>(sb.next_id(), pnames, pexprs, node);
  const auto agg_src = node->outputType();

  std::vector<core::FieldAccessTypedExprPtr> keys;
  for (const auto& g : plan.groups) {
    keys.push_back(field(agg_src->findChild(g), g));
  }
  std::vector<std::string> anames;
  std::vector<core::AggregationNode::Aggregate> aggs;
  std::vector<TypePtr> inter_types;
  for (size_t i = 0; i < plan.aggs.size(); i++) {
    const char* fn = agg_fn(plan.aggs[i].kind);
    if (fn == nullptr) throw Unsupported("aggregate kind");
    std::vector<core::TypedExprPtr> args;
    std::vector<TypePtr> raw;
    if (plan.aggs[i].kind != 3) {
      TypePtr t = agg_src->findChild(agg_in[i]);
      args.push_back(field(t, agg_in[i]));
      raw.push_back(t);
    }
    TypePtr inter = exec::resolveIntermediateType(fn, raw);
    core::AggregationNode::Aggregate a;
    a.call = std::make_shared<core::CallTypedExpr>(inter, std::move(args), fn);
    a.rawInputTypes = raw;
    anames.push_back("_a" + std::to_string(i));
    aggs.push_back(std::move(a));
    inter_types.push_back(inter);
  }
  node = std::make_shared<core::AggregationNode>(
      sb.next_id(), core::AggregationNode::Step::kPartial, keys,
      std::vector<core::FieldAccessTypedExprPtr>{}, anames, aggs, false, false, node);

  // Spark's partial output is groups ++ flattened buffers: a ROW intermediate
  // (decimal sum: sum, isEmpty; avg: sum, count) becomes one column per field.
  std::vector<std::string> onames;
  std::vector<core::TypedExprPtr> oexprs;
  const auto agg_out = node->outputType();
  for (const auto& g : plan.groups) {
    onames.push_back(g);
    oexprs.push_back(field(agg_out->findChild(g), g));
  }
  for (size_t i = 0; i < anames.size(); i++) {
    auto col = field(inter_types[i], anames[i]);
    if (inter_types[i]->kind() == TypeKind::ROW) {
      const auto& rt = inter_types[i]->asRow();
      for (uint32_t c = 0; c < rt.size(); c++) {
        onames.push_back(anames[i] + "_" + std::to_string(c));
        oexprs.push_back(std::make_shared<core::DereferenceTypedExpr>(rt.childAt(c), col, c));
      }
    } else {
      onames.push_back(anames[i]);
      oexprs.push_back(col);
    }
  }
  node = std::make_shared<core::ProjectNode>(sb.next_id(), onames, oexprs, node);
  return BuiltStage{node, std::move(sb.scans)};
}

struct VeloxRun {
  std::shared_ptr<core::QueryCtx> ctx;
  std::shared_ptr<memory::MemoryPool> export_pool;
  std::shared_ptr<exec::Task> task;
  int64_t rows = 0;
  int batches = 0;
};

}  // namespace

void* vegam_velox_start(JNIEnv*, const VegamDecoded& plan, int) {
  try {
    ensure_velox();
    BuiltStage stage = build_stage(plan);
    static std::atomic<uint64_t> seq{0};
    const auto n = seq.fetch_add(1);
    auto run = std::make_unique<VeloxRun>();
    std::unordered_map<std::string, std::string> qcfg;
    qcfg[core::QueryConfig::kPreferredOutputBatchRows] = std::to_string(vegam::kMorselRows);
    run->ctx = core::QueryCtx::create(nullptr, core::QueryConfig{std::move(qcfg)});
    run->export_pool = memory::memoryManager()->addLeafPool("vegam-export-" + std::to_string(n));
    run->task = exec::Task::create(
        "vegam-stage-" + std::to_string(n),
        core::PlanFragment{stage.root},
        0,
        run->ctx,
        exec::Task::ExecutionMode::kSerial);
    size_t nsplits = 0;
    for (auto& s : stage.scans) {
      for (auto& sp : s.splits) {
        run->task->addSplit(s.id, exec::Split(std::move(sp)));
        nsplits++;
      }
      run->task->noMoreSplits(s.id);
    }
    std::cerr << "vegam: kernel=velox stage scans=" << stage.scans.size()
              << " splits=" << nsplits << " builds=" << plan.builds.size()
              << " groups=" << plan.groups.size() << " aggs=" << plan.aggs.size()
              << std::endl;
    return run.release();
  } catch (const Unsupported& e) {
    std::cerr << "vegam: velox skip " << e.what() << std::endl;
    return nullptr;
  } catch (const std::exception& e) {
    std::cerr << "vegam: velox plan failed, fused pipeline fallback: " << e.what()
              << std::endl;
    return nullptr;
  }
}

int vegam_velox_next(void* handle, void* arrow_array, void* arrow_schema) {
  auto* run = static_cast<VeloxRun*>(handle);
  RowVectorPtr v;
  while ((v = run->task->next()) != nullptr && v->size() == 0) {
  }
  if (v == nullptr) {
    return -1;
  }
  ArrowOptions opts;
  opts.flattenDictionary = true;
  opts.flattenConstant = true;
  exportToArrow(v, *static_cast<ArrowArray*>(arrow_array), run->export_pool.get(), opts);
  exportToArrow(v, *static_cast<ArrowSchema*>(arrow_schema), opts);
  run->rows += v->size();
  run->batches++;
  return static_cast<int>(v->size());
}

void vegam_velox_close(void* handle) {
  auto* run = static_cast<VeloxRun*>(handle);
  if (run == nullptr) {
    return;
  }
  if (run->task != nullptr) {
    run->task->requestCancel();
  }
  run->task.reset();
  delete run;
}

#endif
