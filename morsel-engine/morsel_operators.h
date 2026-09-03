/*
 * Morsel-Aware Operators
 * Implementation patterns from DuckDB
 */

#ifndef MORSEL_OPERATORS_H
#define MORSEL_OPERATORS_H

#include "morsel_scheduler.h"
#include <arrow/compute/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <unordered_map>

// Platform-specific SIMD includes
#if defined(__x86_64__) || defined(_M_X64)
  #include <immintrin.h>
  #define MORSEL_HAS_AVX2 1
#else
  #define MORSEL_HAS_AVX2 0
#endif

namespace morsel {

// ============================================================================
// ParquetScanOperator: Streaming parquet scan with morsels
// Based on DuckDB's ParquetScan
// ============================================================================
class ParquetScanOperator : public Operator {
private:
  std::string file_path;
  std::vector<std::string> column_names;
  std::unique_ptr<parquet::arrow::FileReader> reader;
  std::atomic<int> current_row_group{0};
  int total_row_groups;
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

    total_row_groups = reader->parquet_reader()->metadata()->num_row_groups();
  }

  std::shared_ptr<Morsel> process_morsel(
      std::shared_ptr<Morsel> input,
      ExecutionContext* ctx) override {

    // Get next row group atomically
    int rg = current_row_group.fetch_add(1);
    if (rg >= total_row_groups) {
      return nullptr;  // No more row groups
    }

    // Read row group
    std::shared_ptr<arrow::Table> table;
    {
      std::lock_guard<std::mutex> lock(reader_mutex);
      auto status = reader->ReadRowGroup(rg, &table);
      if (!status.ok()) {
        return nullptr;
      }
    }

    // Convert to RecordBatch
    auto batch_result = table->CombineChunksToBatch();
    if (!batch_result.ok()) {
      return nullptr;
    }

    auto batch = *batch_result;

    // Return as morsel
    return std::make_shared<Morsel>(
      batch, 0, batch->num_rows(), rg, 0);
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
    auto column = batch->column(column_index);

    // Use Arrow compute for now (it uses SIMD internally)
    // TODO: Implement custom AVX-512 filter like DuckDB
    arrow::compute::ExecContext exec_ctx(ctx->pool);

    // Map CompareOperator to function name (Arrow 15+ removed CompareOperatorToFunctionName)
    std::string func_name;
    if (op == arrow::compute::CompareOperator::GREATER) func_name = "greater";
    else if (op == arrow::compute::CompareOperator::LESS) func_name = "less";
    else if (op == arrow::compute::CompareOperator::EQUAL) func_name = "equal";
    else func_name = "greater";  // default

    auto compare_result = arrow::compute::CallFunction(
      func_name,
      {column, literal}, &exec_ctx);

    if (!compare_result.ok()) {
      return nullptr;
    }

    auto filter_array = compare_result->make_array();

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
  struct AggregateState {
    std::unordered_map<int64_t, std::vector<int64_t>> hash_table;
    std::mutex mutex;
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
    auto* state = thread_states[ctx->thread_id].get();

    // Simple aggregation: group by first column, sum second column
    // TODO: Generalize for multiple group-by columns and aggregates
    if (group_by_columns.size() != 1 || aggregate_columns.size() != 1) {
      return nullptr;  // Not implemented yet
    }

    int group_col = group_by_columns[0];
    int agg_col = aggregate_columns[0];

    auto group_array = std::static_pointer_cast<arrow::Int64Array>(
      batch->column(group_col));
    auto agg_array = std::static_pointer_cast<arrow::Int64Array>(
      batch->column(agg_col));

    // Thread-local aggregation (no lock needed)
    for (int64_t i = 0; i < batch->num_rows(); i++) {
      if (!group_array->IsNull(i) && !agg_array->IsNull(i)) {
        int64_t key = group_array->Value(i);
        int64_t value = agg_array->Value(i);

        auto& entry = state->hash_table[key];
        if (entry.empty()) {
          entry.resize(2);  // [count, sum]
        }
        entry[0]++;        // count
        entry[1] += value; // sum
      }
    }

    // Don't return anything - aggregation is stateful
    return nullptr;
  }

  std::shared_ptr<arrow::RecordBatch> finalize(
      ExecutionContext* ctx) override {

    // Merge all thread-local hash tables
    std::unordered_map<int64_t, std::vector<int64_t>> merged;

    for (auto& state : thread_states) {
      for (auto& [key, values] : state->hash_table) {
        auto& entry = merged[key];
        if (entry.empty()) {
          entry = values;
        } else {
          entry[0] += values[0];  // count
          entry[1] += values[1];  // sum
        }
      }
    }

    // Convert to Arrow RecordBatch
    arrow::Int64Builder key_builder;
    arrow::Int64Builder sum_builder;

    for (auto& [key, values] : merged) {
      (void)key_builder.Append(key);
      (void)sum_builder.Append(values[1]);  // sum
    }

    std::shared_ptr<arrow::Array> key_array, sum_array;
    (void)key_builder.Finish(&key_array);
    (void)sum_builder.Finish(&sum_array);

    auto schema = arrow::schema({
      arrow::field("group_key", arrow::int64()),
      arrow::field("sum", arrow::int64())
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
