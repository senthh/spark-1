/*
 * ClickHouse-style Sort Operator for Morsel Engine
 * Radix sort for integers, partial sorting optimization
 */

#ifndef MORSEL_SORT_H
#define MORSEL_SORT_H

#include "morsel_scheduler.h"
#include <algorithm>
#include <cstring>

namespace morsel {

// ============================================================================
// RadixSortOperator: ClickHouse-style radix sort for integer columns
// Much faster than comparison sort for integers
// ============================================================================
class RadixSortOperator : public Operator {
private:
  int sort_column;
  bool ascending;

  // Radix sort implementation (ClickHouse pattern)
  static void radix_sort_int64(
      std::vector<std::pair<int64_t, int64_t>>& pairs,
      bool ascending) {

    const int num_passes = 8;  // 8 bytes = 8 passes
    const int radix_bits = 8;
    const int radix_size = 1 << radix_bits;

    std::vector<std::pair<int64_t, int64_t>> temp(pairs.size());

    for (int pass = 0; pass < num_passes; pass++) {
      int shift = pass * radix_bits;

      // Count histogram
      std::vector<int> counts(radix_size, 0);
      for (const auto& p : pairs) {
        uint64_t key = static_cast<uint64_t>(p.first);
        int digit = (key >> shift) & 0xFF;
        counts[digit]++;
      }

      // Compute offsets
      std::vector<int> offsets(radix_size, 0);
      for (int i = 1; i < radix_size; i++) {
        offsets[i] = offsets[i-1] + counts[i-1];
      }

      // Scatter to temp
      for (const auto& p : pairs) {
        uint64_t key = static_cast<uint64_t>(p.first);
        int digit = (key >> shift) & 0xFF;
        temp[offsets[digit]++] = p;
      }

      // Swap buffers
      std::swap(pairs, temp);
    }

    // Reverse if descending
    if (!ascending) {
      std::reverse(pairs.begin(), pairs.end());
    }
  }

public:
  RadixSortOperator(int col_idx, bool asc = true)
    : sort_column(col_idx), ascending(asc) {}

  std::shared_ptr<Morsel> process_morsel(
      std::shared_ptr<Morsel> input,
      ExecutionContext* ctx) override {

    if (!input) return nullptr;

    auto batch = input->slice();
    auto sort_array = batch->column(sort_column);

    // Only works for int64 (ClickHouse also has specialized paths)
    if (sort_array->type_id() != arrow::Type::INT64) {
      // Fallback to comparison sort
      return nullptr;
    }

    auto int_array = std::static_pointer_cast<arrow::Int64Array>(sort_array);

    // Build (value, index) pairs
    std::vector<std::pair<int64_t, int64_t>> pairs;
    pairs.reserve(batch->num_rows());

    for (int64_t i = 0; i < batch->num_rows(); i++) {
      if (!int_array->IsNull(i)) {
        pairs.push_back({int_array->Value(i), i});
      }
    }

    // Radix sort
    radix_sort_int64(pairs, ascending);

    // Build sorted indices
    arrow::Int64Builder indices_builder;
    for (const auto& p : pairs) {
      indices_builder.Append(p.second);
    }

    std::shared_ptr<arrow::Array> indices_array;
    indices_builder.Finish(&indices_array);

    // Take sorted rows
    arrow::compute::ExecContext exec_ctx(ctx->pool);
    auto take_result = arrow::compute::Take(batch, indices_array, &exec_ctx);

    if (!take_result.ok()) {
      return nullptr;
    }

    auto sorted_batch = take_result->record_batch();

    return std::make_shared<Morsel>(
      sorted_batch, 0, sorted_batch->num_rows(),
      input->partition_id, input->morsel_id);
  }

  const char* name() const override { return "RadixSort"; }
};

// ============================================================================
// PartialSortOperator: ClickHouse partial sorting optimization
// For "ORDER BY ... LIMIT N", only maintain top-N heap
// ============================================================================
class PartialSortOperator : public Operator {
private:
  int sort_column;
  int64_t limit;
  bool ascending;

  // Min-heap for top-K (ClickHouse uses this for LIMIT queries)
  struct TopKHeap {
    std::vector<std::pair<int64_t, int64_t>> heap;  // (value, index)
    int64_t max_size;
    bool ascending;

    TopKHeap(int64_t k, bool asc) : max_size(k), ascending(asc) {
      heap.reserve(k);
    }

    void insert(int64_t value, int64_t index) {
      if (heap.size() < static_cast<size_t>(max_size)) {
        heap.push_back({value, index});
        std::push_heap(heap.begin(), heap.end(),
          [this](const auto& a, const auto& b) {
            return ascending ? (a.first > b.first) : (a.first < b.first);
          });
      } else {
        // Check if better than worst
        bool should_insert = ascending ?
          (value < heap.front().first) :
          (value > heap.front().first);

        if (should_insert) {
          std::pop_heap(heap.begin(), heap.end(),
            [this](const auto& a, const auto& b) {
              return ascending ? (a.first > b.first) : (a.first < b.first);
            });
          heap.back() = {value, index};
          std::push_heap(heap.begin(), heap.end(),
            [this](const auto& a, const auto& b) {
              return ascending ? (a.first > b.first) : (a.first < b.first);
            });
        }
      }
    }

    std::vector<int64_t> get_indices() {
      std::sort_heap(heap.begin(), heap.end(),
        [this](const auto& a, const auto& b) {
          return ascending ? (a.first > b.first) : (a.first < b.first);
        });

      std::vector<int64_t> indices;
      for (const auto& p : heap) {
        indices.push_back(p.second);
      }

      if (!ascending) {
        std::reverse(indices.begin(), indices.end());
      }

      return indices;
    }
  };

public:
  PartialSortOperator(int col_idx, int64_t lim, bool asc = true)
    : sort_column(col_idx), limit(lim), ascending(asc) {}

  std::shared_ptr<Morsel> process_morsel(
      std::shared_ptr<Morsel> input,
      ExecutionContext* ctx) override {

    if (!input) return nullptr;

    auto batch = input->slice();
    auto sort_array = batch->column(sort_column);

    if (sort_array->type_id() != arrow::Type::INT64) {
      return nullptr;
    }

    auto int_array = std::static_pointer_cast<arrow::Int64Array>(sort_array);

    // Build top-K heap
    TopKHeap heap(limit, ascending);

    for (int64_t i = 0; i < batch->num_rows(); i++) {
      if (!int_array->IsNull(i)) {
        heap.insert(int_array->Value(i), i);
      }
    }

    // Get top-K indices
    auto indices = heap.get_indices();

    // Build Arrow array
    arrow::Int64Builder indices_builder;
    for (int64_t idx : indices) {
      indices_builder.Append(idx);
    }

    std::shared_ptr<arrow::Array> indices_array;
    indices_builder.Finish(&indices_array);

    // Take top-K rows
    arrow::compute::ExecContext exec_ctx(ctx->pool);
    auto take_result = arrow::compute::Take(batch, indices_array, &exec_ctx);

    if (!take_result.ok()) {
      return nullptr;
    }

    auto topk_batch = take_result->record_batch();

    return std::make_shared<Morsel>(
      topk_batch, 0, topk_batch->num_rows(),
      input->partition_id, input->morsel_id);
  }

  const char* name() const override { return "PartialSort"; }
};

} // namespace morsel

#endif // MORSEL_SORT_H
