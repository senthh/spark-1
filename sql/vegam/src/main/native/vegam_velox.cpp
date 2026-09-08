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

#include <cmath>
#include <iostream>
#include <mutex>

#include "velox/common/memory/Memory.h"
#include "velox/core/Expressions.h"
#include "velox/core/PlanNode.h"
#include "velox/core/QueryCtx.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/parquet/RegisterParquetReader.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/exec/Task.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/vector/DecodedVector.h"

using namespace facebook::velox;

namespace {

void ensure_velox() {
  static std::once_flag once;
  std::call_once(once, [] {
    memory::MemoryManager::initialize({});
    parquet::registerParquetReaderFactory();
    aggregate::prestosql::registerAllAggregateFunctions();
    std::cerr << "vegam: velox memory+parquet+hashagg registered" << std::endl;
  });
}

int child_idx(const RowTypePtr& type, const std::string& name) {
  auto idx = type->getChildIdxIfExists(name);
  if (idx.has_value()) {
    return static_cast<int>(idx.value());
  }
  for (size_t i = 0; i < type->size(); i++) {
    if (type->nameOf(i) == name) {
      return static_cast<int>(i);
    }
  }
  return -1;
}

double decoded_double(DecodedVector& dec, vector_size_t r) {
  const TypePtr& t = dec.base()->type();
  if (t->isShortDecimal()) {
    auto scale = t->asShortDecimal().scale();
    return static_cast<double>(dec.valueAt<int64_t>(r)) /
        std::pow(10.0, static_cast<double>(scale));
  }
  if (t->isLongDecimal()) {
    auto scale = t->asLongDecimal().scale();
    return static_cast<double>(dec.valueAt<int128_t>(r)) /
        std::pow(10.0, static_cast<double>(scale));
  }
  switch (t->kind()) {
    case TypeKind::TINYINT:
      return static_cast<double>(dec.valueAt<int8_t>(r));
    case TypeKind::SMALLINT:
      return static_cast<double>(dec.valueAt<int16_t>(r));
    case TypeKind::INTEGER:
      return static_cast<double>(dec.valueAt<int32_t>(r));
    case TypeKind::BIGINT:
      return static_cast<double>(dec.valueAt<int64_t>(r));
    case TypeKind::REAL:
      return static_cast<double>(dec.valueAt<float>(r));
    case TypeKind::DOUBLE:
      return dec.valueAt<double>(r);
    case TypeKind::HUGEINT:
      return static_cast<double>(dec.valueAt<int128_t>(r));
    default:
      return NAN;
  }
}

void append_row_vector(const RowVectorPtr& vec, VegamTable* out) {
  if (vec == nullptr || vec->size() == 0) {
    return;
  }
  const int cols = static_cast<int>(vec->childrenSize());
  if (out->cols.empty()) {
    out->cols.resize(static_cast<size_t>(cols));
    out->names.resize(static_cast<size_t>(cols));
    auto rowType = std::dynamic_pointer_cast<const RowType>(vec->type());
    for (int c = 0; c < cols; c++) {
      out->names[c] = rowType ? rowType->nameOf(c) : std::to_string(c);
    }
  }
  const int start = out->num_rows;
  const int n = vec->size();
  out->num_rows = start + n;
  for (int c = 0; c < cols; c++) {
    out->cols[c].values.resize(out->num_rows);
    out->cols[c].nulls.resize(out->num_rows);
    out->cols[c].texts.resize(out->num_rows);
    DecodedVector dec(*vec->childAt(c));
    for (int r = 0; r < n; r++) {
      if (dec.isNullAt(r)) {
        out->cols[c].nulls[start + r] = 1;
        out->cols[c].values[start + r] = NAN;
      } else {
        out->cols[c].nulls[start + r] = 0;
        out->cols[c].values[start + r] = decoded_double(dec, r);
      }
    }
  }
}

RowTypePtr project_type(const RowTypePtr& fileType, const std::string& group,
                        const std::string& sumc) {
  int g = child_idx(fileType, group);
  int s = child_idx(fileType, sumc);
  if (g < 0 || s < 0) {
    return nullptr;
  }
  return ROW({group, sumc}, {fileType->childAt(g), fileType->childAt(s)});
}

std::vector<RowVectorPtr> scan_file(
    JNIEnv* env,
    memory::MemoryPool* pool,
    const std::string& path,
    const std::string& group,
    const std::string& sumc) {
  auto readFile = std::make_shared<HadoopReadFile>(env, path);
  dwio::common::ReaderOptions readerOpts(pool);
  auto input = std::make_unique<dwio::common::BufferedInput>(readFile, *pool);
  auto reader = std::make_unique<parquet::ParquetReader>(std::move(input), readerOpts);
  auto projected = project_type(reader->rowType(), group, sumc);
  if (projected == nullptr) {
    std::cerr << "vegam: velox missing columns " << group << "," << sumc
              << " in " << path << std::endl;
    return {};
  }
  auto scanSpec = std::make_shared<common::ScanSpec>("root");
  scanSpec->addAllChildFields(*projected);
  dwio::common::RowReaderOptions rowOpts;
  rowOpts.setScanSpec(scanSpec);
  auto rowReader = reader->createRowReader(rowOpts);
  std::vector<RowVectorPtr> batches;
  VectorPtr batch;
  while (rowReader->next(4096, batch) > 0) {
    auto row = std::dynamic_pointer_cast<RowVector>(batch);
    if (row != nullptr && row->size() > 0) {
      batches.push_back(row);
    }
    batch = nullptr;
  }
  return batches;
}

TypePtr first_child_type(const std::vector<RowVectorPtr>& batches, int channel) {
  for (const auto& b : batches) {
    if (b != nullptr && channel < static_cast<int>(b->childrenSize())) {
      return b->childAt(channel)->type();
    }
  }
  return BIGINT();
}

}  // namespace

bool vegam_velox_scan_hash_agg(
    JNIEnv* env,
    const std::vector<std::string>& files,
    const std::string& group,
    const std::string& sumc,
    VegamTable* out) {
  try {
    ensure_velox();
    auto pool = memory::memoryManager()->addLeafPool("vegam-scan");
    std::vector<RowVectorPtr> batches;
    for (const auto& f : files) {
      auto part = scan_file(env, pool.get(), f, group, sumc);
      batches.insert(batches.end(), part.begin(), part.end());
    }
    if (batches.empty()) {
      std::cerr << "vegam: velox scan produced 0 batches" << std::endl;
      return false;
    }
    auto groupType = first_child_type(batches, 0);
    auto sumType = first_child_type(batches, 1);
    auto values = std::make_shared<core::ValuesNode>("v0", batches);
    core::FieldAccessTypedExprPtr groupExpr =
        std::make_shared<core::FieldAccessTypedExpr>(groupType, group);
    core::FieldAccessTypedExprPtr sumExpr =
        std::make_shared<core::FieldAccessTypedExpr>(sumType, sumc);
    core::AggregationNode::Aggregate agg;
    agg.call = std::make_shared<core::CallTypedExpr>(
        sumType, std::vector<core::TypedExprPtr>{sumExpr}, "sum");
    agg.rawInputTypes = {sumType};
    auto aggNode = std::make_shared<core::AggregationNode>(
        "a0",
        core::AggregationNode::Step::kSingle,
        std::vector<core::FieldAccessTypedExprPtr>{groupExpr},
        std::vector<core::FieldAccessTypedExprPtr>{},
        std::vector<std::string>{"sum_0"},
        std::vector<core::AggregationNode::Aggregate>{agg},
        false,
        false,
        values);
    auto queryCtx = core::QueryCtx::create();
    auto task = exec::Task::create(
        "vegam-hashagg",
        core::PlanFragment{aggNode},
        0,
        queryCtx,
        exec::Task::ExecutionMode::kSerial);
    RowVectorPtr page;
    while ((page = task->next()) != nullptr) {
      append_row_vector(page, out);
    }
    std::cerr << "vegam: kernel=velox-scan-hashagg files=" << files.size()
              << " groups=" << out->num_rows << std::endl;
    return out->num_rows >= 0;
  } catch (const std::exception& e) {
    std::cerr << "vegam: velox scan+hashagg failed: " << e.what() << std::endl;
    return false;
  }
}

void vegam_velox_hash_agg(
    VegamTable* in,
    const std::vector<int>&,
    const std::vector<int>&,
    const std::vector<int>&,
    VegamTable* out) {
  // Old hook kept for ABI. Scan+HashAgg is vegam_velox_scan_hash_agg.
  if (out != nullptr && in != nullptr) {
    *out = *in;
  }
}

#endif
