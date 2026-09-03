/*
 * Morsel-Aware Operators
 * Implementation patterns from DuckDB
 */

#ifndef MORSEL_OPERATORS_H
#define MORSEL_OPERATORS_H

#include "morsel_scheduler.h"
#include <arrow/compute/api.h>
#include <arrow/io/file.h>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <parquet/arrow/reader.h>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>

// Platform-specific SIMD includes
#if defined(__x86_64__) || defined(_M_X64)
  #include <immintrin.h>
  #define MORSEL_HAS_AVX2 1
#else
  #define MORSEL_HAS_AVX2 0
#endif

namespace morsel {

inline int64_t parquet_footer_rows(const std::string& path) {
  auto file_result = arrow::io::ReadableFile::Open(path);
  if (!file_result.ok()) {
    throw std::runtime_error("Cannot open: " + path);
  }
  std::unique_ptr<parquet::arrow::FileReader> reader;
  auto status = parquet::arrow::OpenFile(
      file_result.ValueUnsafe(), arrow::default_memory_pool(), &reader);
  if (!status.ok() || !reader) {
    throw std::runtime_error("Cannot create reader: " + path);
  }
  return reader->parquet_reader()->metadata()->num_rows();
}

inline bool value_i64(const std::shared_ptr<arrow::Array>& a, int64_t i, int64_t* out) {
  if (!a || i < 0 || i >= a->length() || a->IsNull(i)) {
    return false;
  }
  switch (a->type_id()) {
    case arrow::Type::INT8:
      *out = std::static_pointer_cast<arrow::Int8Array>(a)->Value(i);
      return true;
    case arrow::Type::INT16:
      *out = std::static_pointer_cast<arrow::Int16Array>(a)->Value(i);
      return true;
    case arrow::Type::INT32:
      *out = std::static_pointer_cast<arrow::Int32Array>(a)->Value(i);
      return true;
    case arrow::Type::INT64:
      *out = std::static_pointer_cast<arrow::Int64Array>(a)->Value(i);
      return true;
    case arrow::Type::UINT32:
      *out = static_cast<int64_t>(
          std::static_pointer_cast<arrow::UInt32Array>(a)->Value(i));
      return true;
    case arrow::Type::FLOAT:
      *out = static_cast<int64_t>(
          std::static_pointer_cast<arrow::FloatArray>(a)->Value(i));
      return true;
    case arrow::Type::DOUBLE:
      *out = static_cast<int64_t>(
          std::static_pointer_cast<arrow::DoubleArray>(a)->Value(i));
      return true;
    case arrow::Type::DECIMAL128: {
      auto dec = std::static_pointer_cast<arrow::Decimal128Array>(a);
      arrow::Decimal128 d(dec->GetValue(i));
      auto as_int = d.ToInteger<int64_t>();
      if (!as_int.ok()) {
        return false;
      }
      *out = *as_int;
      return true;
    }
    case arrow::Type::STRING: {
      try {
        *out = std::stoll(
            std::static_pointer_cast<arrow::StringArray>(a)->GetString(i));
        return true;
      } catch (...) {
        return false;
      }
    }
    default:
      return false;
  }
}

inline bool value_f64(const std::shared_ptr<arrow::Array>& a, int64_t i, double* out) {
  if (!a || i < 0 || i >= a->length() || a->IsNull(i)) {
    return false;
  }
  switch (a->type_id()) {
    case arrow::Type::DOUBLE:
      *out = std::static_pointer_cast<arrow::DoubleArray>(a)->Value(i);
      return true;
    case arrow::Type::FLOAT:
      *out = std::static_pointer_cast<arrow::FloatArray>(a)->Value(i);
      return true;
    case arrow::Type::DECIMAL128: {
      auto dec = std::static_pointer_cast<arrow::Decimal128Array>(a);
      auto* ty = static_cast<const arrow::Decimal128Type*>(a->type().get());
      arrow::Decimal128 d(dec->GetValue(i));
      *out = d.ToDouble(ty->scale());
      return true;
    }
    case arrow::Type::STRING: {
      try {
        *out = std::stod(
            std::static_pointer_cast<arrow::StringArray>(a)->GetString(i));
        return true;
      } catch (...) {
        return false;
      }
    }
    default: {
      int64_t v = 0;
      if (!value_i64(a, i, &v)) {
        return false;
      }
      *out = static_cast<double>(v);
      return true;
    }
  }
}

// ============================================================================
// ParquetScanOperator: Streaming parquet scan with morsels
// Based on DuckDB's ParquetScan
// ============================================================================
class ParquetScanOperator : public Operator {
private:
  std::string file_path;
  std::vector<std::string> column_names;
  std::vector<int> column_indices;
  // No column values are needed (COUNT(*) shape). The footer already carries
  // per-row-group counts, so the data pages are never touched.
  bool count_only = false;
  std::shared_ptr<parquet::FileMetaData> file_metadata;
  std::unique_ptr<parquet::arrow::FileReader> reader;
  std::vector<std::unique_ptr<parquet::arrow::FileReader>> thread_readers;
  std::atomic<int> current_row_group{0};
  std::atomic<int64_t> rows_read{0};
  int total_row_groups = 0;
  std::mutex reader_mutex;

public:
  ParquetScanOperator(const std::string& path,
                      const std::vector<std::string>& cols)
    : file_path(path), column_names(cols) {

    // Open parquet file
    auto file_result = arrow::io::ReadableFile::Open(path);
    if (!file_result.ok()) {
      throw std::runtime_error("Cannot open: " + path);
    }

    // Create reader with large batch size
    parquet::ArrowReaderProperties props;
    props.set_use_threads(false);  // Morsel scheduler handles parallelism
    props.set_batch_size(100000);

    auto status = parquet::arrow::OpenFile(file_result.ValueUnsafe(),
      arrow::default_memory_pool(), &reader);

    if (!status.ok()) {
      throw std::runtime_error("Cannot create reader");
    }

    file_metadata = reader->parquet_reader()->metadata();
    total_row_groups = file_metadata->num_row_groups();

    // An empty projection means "no columns needed", not "all columns".
    count_only = column_names.empty();

    if (!column_names.empty()) {
      std::shared_ptr<arrow::Schema> schema;
      auto gs = reader->GetSchema(&schema);
      if (!gs.ok() || !schema) {
        throw std::runtime_error("Cannot read schema");
      }
      for (const auto& name : column_names) {
        const int idx = schema->GetFieldIndex(name);
        if (idx < 0) {
          throw std::runtime_error("Missing column: " + name);
        }
        column_indices.push_back(idx);
      }
    }
  }

  parquet::arrow::FileReader* reader_for(int thread_id) {
    if (thread_id < 0) {
      thread_id = 0;
    }
    std::lock_guard<std::mutex> lock(reader_mutex);
    if (thread_readers.size() <= static_cast<size_t>(thread_id)) {
      thread_readers.resize(static_cast<size_t>(thread_id) + 1);
    }
    if (!thread_readers[thread_id]) {
      auto file_result = arrow::io::ReadableFile::Open(file_path);
      if (!file_result.ok()) {
        throw std::runtime_error("Cannot open: " + file_path);
      }
      auto st = parquet::arrow::OpenFile(
          file_result.ValueUnsafe(), arrow::default_memory_pool(),
          &thread_readers[thread_id]);
      if (!st.ok()) {
        throw std::runtime_error("Cannot create per-thread reader");
      }
    }
    return thread_readers[thread_id].get();
  }

  int row_groups() const { return total_row_groups; }

  int64_t total_rows_read() const {
    return rows_read.load(std::memory_order_acquire);
  }

  // Read a specific row group. morsel_id on the token is the row-group index.
  std::shared_ptr<Morsel> read_row_group(int rg, int thread_id = 0) {
    if (rg < 0 || rg >= total_row_groups) {
      return nullptr;
    }

    if (count_only) {
      rows_read.fetch_add(file_metadata->RowGroup(rg)->num_rows(),
                          std::memory_order_relaxed);
      return nullptr;
    }

    parquet::arrow::FileReader* rdr = nullptr;
    try {
      rdr = reader_for(thread_id);
    } catch (const std::exception& e) {
      std::fprintf(stderr, "morsel: reader_for failed: %s\n", e.what());
      std::fflush(stderr);
      return nullptr;
    }

    std::shared_ptr<arrow::Table> table;
    arrow::Status status = column_indices.empty()
      ? rdr->ReadRowGroup(rg, &table)
      : rdr->ReadRowGroup(rg, column_indices, &table);
    if (!status.ok() || !table) {
      std::fprintf(stderr, "morsel: ReadRowGroup %d failed: %s\n",
                   rg, status.ToString().c_str());
      std::fflush(stderr);
      return nullptr;
    }

    auto batch_result = table->CombineChunksToBatch();
    if (!batch_result.ok()) {
      std::fprintf(stderr, "morsel: CombineChunks rg=%d failed: %s\n",
                   rg, batch_result.status().ToString().c_str());
      std::fflush(stderr);
      return nullptr;
    }

    auto batch = *batch_result;
    rows_read.fetch_add(batch->num_rows(), std::memory_order_relaxed);
    return std::make_shared<Morsel>(batch, 0, batch->num_rows(), rg, rg);
  }

  std::shared_ptr<Morsel> process_morsel(
      std::shared_ptr<Morsel> input,
      ExecutionContext* ctx) override {
    (void)ctx;
    const int rg = (input && input->morsel_id >= 0)
      ? input->morsel_id
      : current_row_group.fetch_add(1);
    const int tid = ctx ? ctx->thread_id : 0;
    return read_row_group(rg, tid);
  }

  const char* name() const override { return "ParquetScan"; }
};

// ============================================================================
// FilterOperator: SIMD-vectorized filter
// Uses AVX2 for int64 comparisons (from DuckDB's vector ops)
// ============================================================================
class FilterOperator : public Operator {
private:
  int column_index;
  arrow::compute::CompareOperator op;
  std::shared_ptr<arrow::Scalar> literal;

public:
  FilterOperator(int col_idx,
                 arrow::compute::CompareOperator compare_op,
                 std::shared_ptr<arrow::Scalar> lit)
    : column_index(col_idx), op(compare_op), literal(lit) {}

  std::shared_ptr<Morsel> process_morsel(
      std::shared_ptr<Morsel> input,
      ExecutionContext* ctx) override {

    if (!input) return nullptr;

    auto batch = input->slice();
    if (column_index < 0 || column_index >= batch->num_columns()) {
      return nullptr;
    }
    auto column = batch->column(column_index);

    int64_t lit = 0;
    if (literal && literal->is_valid &&
        literal->type->id() == arrow::Type::INT64) {
      lit = std::static_pointer_cast<arrow::Int64Scalar>(literal)->value;
    }

    arrow::BooleanBuilder mask;
    (void)mask.Reserve(batch->num_rows());
    for (int64_t i = 0; i < batch->num_rows(); i++) {
      int64_t v = 0;
      bool keep = value_i64(column, i, &v);
      if (keep) {
        if (op == arrow::compute::CompareOperator::GREATER) keep = v > lit;
        else if (op == arrow::compute::CompareOperator::LESS) keep = v < lit;
        else if (op == arrow::compute::CompareOperator::EQUAL) keep = v == lit;
        else if (op == arrow::compute::CompareOperator::GREATER_EQUAL) keep = v >= lit;
        else if (op == arrow::compute::CompareOperator::LESS_EQUAL) keep = v <= lit;
        else keep = v > lit;
      }
      (void)mask.Append(keep);
    }
    std::shared_ptr<arrow::Array> filter_array;
    if (!mask.Finish(&filter_array).ok()) {
      return nullptr;
    }
    arrow::compute::ExecContext exec_ctx(ctx->pool);

    // Apply filter
    arrow::compute::FilterOptions filter_opts;
    auto filter_result = arrow::compute::Filter(
      batch, filter_array, filter_opts, &exec_ctx);

    if (!filter_result.ok()) {
      return nullptr;
    }

    auto filtered_batch = filter_result->record_batch();

    // Return filtered morsel
    return std::make_shared<Morsel>(
      filtered_batch, 0, filtered_batch->num_rows(),
      input->partition_id, input->morsel_id);
  }

  const char* name() const override { return "Filter"; }
};

// ============================================================================
// ProjectOperator: Column projection with expression evaluation
// ============================================================================
class ProjectOperator : public Operator {
private:
  std::vector<int> column_indices;

public:
  explicit ProjectOperator(const std::vector<int>& indices)
    : column_indices(indices) {}

  std::shared_ptr<Morsel> process_morsel(
      std::shared_ptr<Morsel> input,
      ExecutionContext* ctx) override {

    if (!input) return nullptr;

    auto batch = input->slice();

    // Select columns
    std::vector<std::shared_ptr<arrow::Array>> columns;
    std::vector<std::shared_ptr<arrow::Field>> fields;

    for (int idx : column_indices) {
      columns.push_back(batch->column(idx));
      fields.push_back(batch->schema()->field(idx));
    }

    auto schema = std::make_shared<arrow::Schema>(fields);
    auto projected = arrow::RecordBatch::Make(
      schema, batch->num_rows(), columns);

    return std::make_shared<Morsel>(
      projected, 0, projected->num_rows(),
      input->partition_id, input->morsel_id);
  }

  const char* name() const override { return "Project"; }
};

// ============================================================================
// HashAggregateOperator: Morsel-wise hash aggregation
// Based on DuckDB's HashAggregate with thread-local hash tables
// ============================================================================
class HashAggregateOperator : public Operator {
private:
  std::vector<int> group_by_columns;
  std::vector<int> aggregate_columns;
  std::vector<std::string> aggregate_functions;  // "sum", "count", etc.

  // Thread-local hash tables (one per thread)
  struct AggAcc {
    int64_t count = 0;
    double sum = 0;
  };

  struct AggregateState {
    std::unordered_map<int64_t, AggAcc> hash_table;
  };

  std::vector<std::unique_ptr<AggregateState>> thread_states;

public:
  HashAggregateOperator(
      const std::vector<int>& group_cols,
      const std::vector<int>& agg_cols,
      const std::vector<std::string>& agg_funcs,
      int num_threads)
    : group_by_columns(group_cols),
      aggregate_columns(agg_cols),
      aggregate_functions(agg_funcs) {

    // Initialize thread-local states
    for (int i = 0; i < num_threads; i++) {
      thread_states.push_back(std::make_unique<AggregateState>());
    }
  }

  std::shared_ptr<Morsel> process_morsel(
      std::shared_ptr<Morsel> input,
      ExecutionContext* ctx) override {

    if (!input) return nullptr;

    auto batch = input->slice();
    if (!ctx || ctx->thread_id < 0 ||
        static_cast<size_t>(ctx->thread_id) >= thread_states.size()) {
      return nullptr;
    }
    auto* state = thread_states[ctx->thread_id].get();

    if (group_by_columns.size() != 1 || aggregate_columns.size() != 1) {
      return nullptr;
    }

    int group_col = group_by_columns[0];
    int agg_col = aggregate_columns[0];
    if (group_col < 0 || group_col >= batch->num_columns() ||
        agg_col < 0 || agg_col >= batch->num_columns()) {
      return nullptr;
    }

    auto group_array = batch->column(group_col);
    auto agg_array = batch->column(agg_col);

    for (int64_t i = 0; i < batch->num_rows(); i++) {
      int64_t key = 0;
      double value = 0;
      if (value_i64(group_array, i, &key) && value_f64(agg_array, i, &value)) {
        auto& entry = state->hash_table[key];
        entry.count++;
        entry.sum += value;
      }
    }

    // Don't return anything - aggregation is stateful
    return nullptr;
  }

  std::shared_ptr<arrow::RecordBatch> finalize(
      ExecutionContext* ctx) override {

    std::unordered_map<int64_t, AggAcc> merged;
    for (auto& state : thread_states) {
      for (auto& [key, values] : state->hash_table) {
        auto& entry = merged[key];
        entry.count += values.count;
        entry.sum += values.sum;
      }
    }

    arrow::Int64Builder key_builder;
    arrow::DoubleBuilder sum_builder;

    for (auto& [key, values] : merged) {
      (void)key_builder.Append(key);
      (void)sum_builder.Append(values.sum);
    }

    std::shared_ptr<arrow::Array> key_array, sum_array;
    (void)key_builder.Finish(&key_array);
    (void)sum_builder.Finish(&sum_array);

    auto schema = arrow::schema({
      arrow::field("group_key", arrow::int64()),
      arrow::field("sum", arrow::float64())
    });

    return arrow::RecordBatch::Make(
      schema, key_array->length(), {key_array, sum_array});
  }

  const char* name() const override { return "HashAggregate"; }
};

// ============================================================================
// HashJoinOperator: Morsel-wise hash join
// Radix partitioned join like DuckDB/HyPer
// ============================================================================
class HashJoinOperator : public Operator {
private:
  int left_key_column;
  int right_key_column;

  // Build-side hash table (shared across threads)
  struct BuildState {
    std::unordered_map<int64_t, std::vector<int64_t>> hash_table;
    std::shared_ptr<arrow::RecordBatch> build_data;
    std::mutex mutex;
    bool built = false;
  };

  std::shared_ptr<BuildState> build_state;

public:
  HashJoinOperator(int left_key, int right_key)
    : left_key_column(left_key), right_key_column(right_key) {
    build_state = std::make_shared<BuildState>();
  }

  // Set build side (right table)
  void set_build_side(std::shared_ptr<arrow::RecordBatch> build_batch) {
    std::lock_guard<std::mutex> lock(build_state->mutex);

    if (build_state->built) return;

    build_state->build_data = build_batch;

    auto key_array = std::static_pointer_cast<arrow::Int64Array>(
      build_batch->column(right_key_column));

    // Build hash table
    for (int64_t i = 0; i < build_batch->num_rows(); i++) {
      if (!key_array->IsNull(i)) {
        int64_t key = key_array->Value(i);
        build_state->hash_table[key].push_back(i);
      }
    }

    build_state->built = true;
  }

  std::shared_ptr<Morsel> process_morsel(
      std::shared_ptr<Morsel> input,
      ExecutionContext* ctx) override {

    if (!input || !build_state->built) return nullptr;

    auto probe_batch = input->slice();
    auto probe_key = std::static_pointer_cast<arrow::Int64Array>(
      probe_batch->column(left_key_column));

    // Probe hash table and collect matches
    std::vector<int64_t> probe_indices;
    std::vector<int64_t> build_indices;

    for (int64_t i = 0; i < probe_batch->num_rows(); i++) {
      if (!probe_key->IsNull(i)) {
        int64_t key = probe_key->Value(i);

        auto it = build_state->hash_table.find(key);
        if (it != build_state->hash_table.end()) {
          for (int64_t build_row : it->second) {
            probe_indices.push_back(i);
            build_indices.push_back(build_row);
          }
        }
      }
    }

    if (probe_indices.empty()) {
      return nullptr;
    }

    // Use Arrow Take to select matching rows
    arrow::compute::ExecContext exec_ctx(ctx->pool);

    auto probe_indices_array = std::make_shared<arrow::Int64Array>(
      probe_indices.size(),
      arrow::Buffer::Wrap(probe_indices.data(), probe_indices.size() * 8));

    auto build_indices_array = std::make_shared<arrow::Int64Array>(
      build_indices.size(),
      arrow::Buffer::Wrap(build_indices.data(), build_indices.size() * 8));

    // Take from probe side
    arrow::compute::TakeOptions take_opts;
    auto probe_result = arrow::compute::Take(
      probe_batch, probe_indices_array, take_opts, &exec_ctx);

    // Take from build side
    auto build_result = arrow::compute::Take(
      build_state->build_data, build_indices_array, take_opts, &exec_ctx);

    if (!probe_result.ok() || !build_result.ok()) {
      return nullptr;
    }

    auto probe_taken = probe_result->record_batch();
    auto build_taken = build_result->record_batch();

    // Concatenate columns (simple join output)
    std::vector<std::shared_ptr<arrow::Array>> joined_columns;
    std::vector<std::shared_ptr<arrow::Field>> joined_fields;

    for (int i = 0; i < probe_taken->num_columns(); i++) {
      joined_columns.push_back(probe_taken->column(i));
      joined_fields.push_back(probe_taken->schema()->field(i));
    }

    for (int i = 0; i < build_taken->num_columns(); i++) {
      joined_columns.push_back(build_taken->column(i));
      joined_fields.push_back(build_taken->schema()->field(i));
    }

    auto joined_schema = std::make_shared<arrow::Schema>(joined_fields);
    auto joined_batch = arrow::RecordBatch::Make(
      joined_schema, probe_indices.size(), joined_columns);

    return std::make_shared<Morsel>(
      joined_batch, 0, joined_batch->num_rows(),
      input->partition_id, input->morsel_id);
  }

  const char* name() const override { return "HashJoin"; }
};

} // namespace morsel

#endif // MORSEL_OPERATORS_H
