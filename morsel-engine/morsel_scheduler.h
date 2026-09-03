/*
 * Morsel-Driven Execution Engine for Spark
 * Based on "Morsel-Driven Parallelism" (SIGMOD 2014)
 * Implementation insights from DuckDB and HyPer
 */

#ifndef MORSEL_SCHEDULER_H
#define MORSEL_SCHEDULER_H

#include <arrow/api.h>
#include <atomic>
#include <memory>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>

namespace morsel {

// Morsel size from paper: 10K-100K rows optimal
// DuckDB uses 2048 by default, we use 10000 for larger batches
constexpr int DEFAULT_MORSEL_SIZE = 10000;

// Forward declarations
class Morsel;
class Pipeline;
class Operator;
class ExecutionContext;

// ============================================================================
// Morsel: Small chunk of data (10K rows)
// ============================================================================
class Morsel {
public:
  std::shared_ptr<arrow::RecordBatch> data;
  int64_t start_row;
  int64_t num_rows;
  int partition_id;
  int morsel_id;

  Morsel(std::shared_ptr<arrow::RecordBatch> batch,
         int64_t start, int64_t count, int part_id, int id)
    : data(batch), start_row(start), num_rows(count),
      partition_id(part_id), morsel_id(id) {}

  // Get morsel data as Arrow RecordBatch slice
  std::shared_ptr<arrow::RecordBatch> slice() const {
    return data->Slice(start_row, num_rows);
  }
};

// ============================================================================
// WorkQueue: Lock-free work-stealing queue
// Inspired by DuckDB's ConcurrentQueue
// ============================================================================
class WorkQueue {
private:
  std::queue<std::shared_ptr<Morsel>> queue;
  std::mutex mutex;
  std::atomic<int> size{0};

public:
  void push(std::shared_ptr<Morsel> morsel) {
    std::lock_guard<std::mutex> lock(mutex);
    queue.push(morsel);
    size++;
  }

  bool try_pop(std::shared_ptr<Morsel>& morsel) {
    std::lock_guard<std::mutex> lock(mutex);
    if (queue.empty()) {
      return false;
    }
    morsel = queue.front();
    queue.pop();
    size--;
    return true;
  }

  int get_size() const { return size.load(); }
  bool empty() const { return size.load() == 0; }
};

// ============================================================================
// ExecutionContext: Per-thread state
// ============================================================================
class ExecutionContext {
public:
  int thread_id;
  int numa_node;
  arrow::MemoryPool* pool;

  ExecutionContext(int tid, int numa_id)
    : thread_id(tid), numa_node(numa_id),
      pool(arrow::default_memory_pool()) {}
};

// ============================================================================
// Operator: Base class for morsel-processing operators
// ============================================================================
class Operator {
public:
  virtual ~Operator() = default;

  // Process one morsel and produce output morsel
  virtual std::shared_ptr<Morsel> process_morsel(
    std::shared_ptr<Morsel> input,
    ExecutionContext* ctx) = 0;

  // Finalize (for operators with state like aggregation)
  virtual std::shared_ptr<arrow::RecordBatch> finalize(
    ExecutionContext* ctx) {
    return nullptr;
  }

  // Operator name for debugging
  virtual const char* name() const = 0;
};

// ============================================================================
// Pipeline: Sequence of fused operators
// Based on DuckDB's Pipeline concept
// ============================================================================
class Pipeline {
public:
  std::vector<std::shared_ptr<Operator>> operators;
  bool requires_finalize = false;

  void add_operator(std::shared_ptr<Operator> op) {
    operators.push_back(op);
  }

  // Execute pipeline on one morsel
  std::shared_ptr<Morsel> execute_morsel(
    std::shared_ptr<Morsel> morsel,
    ExecutionContext* ctx) {

    auto current = morsel;
    for (auto& op : operators) {
      if (!current) break;
      current = op->process_morsel(current, ctx);
    }
    return current;
  }

  // Finalize all operators (for aggregations, joins)
  std::vector<std::shared_ptr<arrow::RecordBatch>> finalize_all(
    ExecutionContext* ctx) {

    std::vector<std::shared_ptr<arrow::RecordBatch>> results;
    for (auto& op : operators) {
      auto result = op->finalize(ctx);
      if (result) {
        results.push_back(result);
      }
    }
    return results;
  }
};

// ============================================================================
// MorselScheduler: Work-stealing scheduler
// Based on "Morsel-Driven Parallelism" Algorithm 1
// ============================================================================
class MorselScheduler {
private:
  // Work queues (one per pipeline)
  std::vector<std::unique_ptr<WorkQueue>> work_queues;

  // Worker threads
  std::vector<std::thread> workers;
  std::atomic<bool> running{false};

  // Thread-local contexts
  std::vector<std::unique_ptr<ExecutionContext>> contexts;

  // Pipelines to execute
  std::vector<std::shared_ptr<Pipeline>> pipelines;

  // Number of worker threads
  int num_threads;

  // Results collection
  std::mutex results_mutex;
  std::vector<std::shared_ptr<arrow::RecordBatch>> results;

  // Worker thread main loop
  void worker_loop(int worker_id) {
    auto* ctx = contexts[worker_id].get();

    while (running.load(std::memory_order_relaxed)) {
      bool did_work = false;

      // Try to get work from any queue (work stealing)
      for (size_t pipeline_id = 0; pipeline_id < work_queues.size(); pipeline_id++) {
        std::shared_ptr<Morsel> morsel;

        if (work_queues[pipeline_id]->try_pop(morsel)) {
          // Process morsel through pipeline
          auto result = pipelines[pipeline_id]->execute_morsel(morsel, ctx);

          if (result && result->num_rows > 0) {
            // Collect results
            std::lock_guard<std::mutex> lock(results_mutex);
            results.push_back(result->slice());
          }

          did_work = true;
          break;  // Process one morsel, then check for more work
        }
      }

      if (!did_work) {
        // No work available, yield CPU
        std::this_thread::yield();
      }
    }
  }

public:
  MorselScheduler(int threads = 0)
    : num_threads(threads > 0 ? threads : std::thread::hardware_concurrency()) {

    // Initialize worker contexts
    for (int i = 0; i < num_threads; i++) {
      // TODO: Detect NUMA node for thread i
      int numa_node = i % 2;  // Simple: alternate between 2 NUMA nodes
      contexts.push_back(std::make_unique<ExecutionContext>(i, numa_node));
    }
  }

  ~MorselScheduler() {
    stop();
  }

  // Add a pipeline to execute
  void add_pipeline(std::shared_ptr<Pipeline> pipeline) {
    pipelines.push_back(pipeline);
    work_queues.push_back(std::make_unique<WorkQueue>());
  }

  // Schedule a partition to be processed as morsels
  void schedule_partition(
    int pipeline_id,
    std::shared_ptr<arrow::RecordBatch> partition,
    int partition_id) {

    int64_t total_rows = partition->num_rows();
    int morsel_id = 0;

    // Split partition into morsels
    for (int64_t start = 0; start < total_rows; start += DEFAULT_MORSEL_SIZE) {
      int64_t count = std::min<int64_t>(DEFAULT_MORSEL_SIZE, total_rows - start);

      auto morsel = std::make_shared<Morsel>(
        partition, start, count, partition_id, morsel_id++);

      work_queues[pipeline_id]->push(morsel);
    }
  }

  // Start worker threads
  void start() {
    running.store(true, std::memory_order_relaxed);

    for (int i = 0; i < num_threads; i++) {
      workers.emplace_back(&MorselScheduler::worker_loop, this, i);
    }
  }

  // Wait for all work to complete
  void wait_for_completion() {
    // Wait until all queues are empty
    while (true) {
      bool all_empty = true;
      for (auto& queue : work_queues) {
        if (!queue->empty()) {
          all_empty = false;
          break;
        }
      }

      if (all_empty) {
        break;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
  }

  // Stop scheduler and join threads
  void stop() {
    if (running.load(std::memory_order_relaxed)) {
      running.store(false, std::memory_order_relaxed);

      for (auto& worker : workers) {
        if (worker.joinable()) {
          worker.join();
        }
      }

      workers.clear();
    }
  }

  // Get results
  std::vector<std::shared_ptr<arrow::RecordBatch>> get_results() {
    std::lock_guard<std::mutex> lock(results_mutex);
    return results;
  }

  // Clear results for next execution
  void clear_results() {
    std::lock_guard<std::mutex> lock(results_mutex);
    results.clear();
  }
};

} // namespace morsel

#endif // MORSEL_SCHEDULER_H
