/*
 * Test Morsel-Driven Execution
 * Validates scheduler and operators work correctly
 */

#include "morsel_scheduler.h"
#include "morsel_operators.h"
#include <iostream>
#include <chrono>

using namespace morsel;
using namespace std::chrono;

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <parquet_file> [num_threads]\n";
    return 1;
  }

  std::string file_path = argv[1];
  int num_threads = argc > 2 ? std::atoi(argv[2]) : std::thread::hardware_concurrency();

  std::cout << "=== Morsel-Driven Execution Test ===\n";
  std::cout << "File: " << file_path << "\n";
  std::cout << "Threads: " << num_threads << "\n";
  std::cout << "Morsel size: " << DEFAULT_MORSEL_SIZE << " rows\n\n";

  try {
    // Create scheduler
    MorselScheduler scheduler(num_threads);

    // Create pipeline: Scan -> Filter -> Project
    auto pipeline = std::make_shared<Pipeline>();

    // Scan operator
    std::vector<std::string> columns = {
      "ss_sold_date_sk", "ss_item_sk", "ss_quantity", "ss_sales_price"
    };
    auto scan = std::make_shared<ParquetScanOperator>(file_path, columns);
    pipeline->add_operator(scan);

    // Filter: ss_quantity > 10
    auto filter_literal = std::make_shared<arrow::Int64Scalar>(10);
    auto filter = std::make_shared<FilterOperator>(
      2,  // ss_quantity column
      arrow::compute::CompareOperator::GREATER,
      filter_literal);
    pipeline->add_operator(filter);

    // Project: select first 3 columns
    auto project = std::make_shared<ProjectOperator>(
      std::vector<int>{0, 1, 2});
    pipeline->add_operator(project);

    // Add pipeline
    scheduler.add_pipeline(pipeline);

    // Benchmark execution
    auto start = high_resolution_clock::now();

    // Start scheduler
    scheduler.start();

    // Wait for completion
    scheduler.wait_for_completion();

    auto end = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end - start).count();

    // Get results
    auto results = scheduler.get_results();

    std::cout << "=== Results ===\n";
    std::cout << "Execution time: " << duration << " ms\n";
    std::cout << "Result batches: " << results.size() << "\n";

    int64_t total_rows = 0;
    for (auto& batch : results) {
      total_rows += batch->num_rows();
    }

    std::cout << "Total rows: " << total_rows << "\n";

    if (!results.empty()) {
      std::cout << "Schema: " << results[0]->schema()->ToString() << "\n";
      std::cout << "\nFirst batch (up to 10 rows):\n";

      auto batch = results[0];
      int rows_to_show = std::min<int>(10, batch->num_rows());

      for (int i = 0; i < rows_to_show; i++) {
        for (int col = 0; col < batch->num_columns(); col++) {
          auto array = batch->column(col);
          if (array->type()->id() == arrow::Type::INT64) {
            auto int_array = std::static_pointer_cast<arrow::Int64Array>(array);
            std::cout << int_array->Value(i) << "\t";
          } else if (array->type()->id() == arrow::Type::INT32) {
            auto int_array = std::static_pointer_cast<arrow::Int32Array>(array);
            std::cout << int_array->Value(i) << "\t";
          } else {
            std::cout << "?\t";
          }
        }
        std::cout << "\n";
      }
    }

    std::cout << "\n=== Test PASSED ===\n";
    return 0;

  } catch (const std::exception& e) {
    std::cerr << "ERROR: " << e.what() << "\n";
    return 1;
  }
}
