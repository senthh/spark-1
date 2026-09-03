/*
 * JNI Bridge for Morsel-Driven Execution
 * Minimal JNI crossings with large result batches
 */

#include "morsel_scheduler.h"
#include "morsel_operators.h"
#include <jni.h>
#include <cstdio>

using namespace morsel;

// Global scheduler (one per executor JVM)
static thread_local std::unique_ptr<MorselScheduler> g_scheduler;

extern "C" {

// Initialize morsel scheduler
JNIEXPORT jlong JNICALL
Java_org_apache_spark_sql_execution_MorselEngine_initScheduler(
    JNIEnv* env, jclass, jint num_threads) {

  if (!g_scheduler) {
    g_scheduler = std::make_unique<MorselScheduler>(num_threads);
  }

  return reinterpret_cast<jlong>(g_scheduler.get());
}

// Scan parquet file with morsel-driven parallelism
JNIEXPORT jlong JNICALL
Java_org_apache_spark_sql_execution_MorselEngine_scanParquet(
    JNIEnv* env,
    jclass,
    jlong scheduler_handle,
    jstring jpath,
    jobjectArray jcolumns,
    jint jfilter_col,
    jlong jfilter_value) {

  auto* scheduler = reinterpret_cast<MorselScheduler*>(scheduler_handle);

  const char* path = env->GetStringUTFChars(jpath, nullptr);
  jsize num_cols = env->GetArrayLength(jcolumns);

  std::vector<std::string> columns;
  for (jsize i = 0; i < num_cols; i++) {
    auto jcol = (jstring)env->GetObjectArrayElement(jcolumns, i);
    const char* col = env->GetStringUTFChars(jcol, nullptr);
    columns.push_back(col);
    env->ReleaseStringUTFChars(jcol, col);
  }

  try {
    // Create pipeline: Scan -> Filter
    auto pipeline = std::make_shared<Pipeline>();

    // Add scan operator
    auto scan_op = std::make_shared<ParquetScanOperator>(path, columns);
    pipeline->add_operator(scan_op);

    // Add filter if specified
    if (jfilter_col >= 0) {
      auto filter_literal = std::make_shared<arrow::Int64Scalar>(jfilter_value);
      auto filter_op = std::make_shared<FilterOperator>(
        jfilter_col,
        arrow::compute::CompareOperator::GREATER,
        filter_literal);
      pipeline->add_operator(filter_op);
    }

    // Add pipeline to scheduler
    scheduler->add_pipeline(pipeline);

    // Start execution
    scheduler->start();

    // Wait for completion
    scheduler->wait_for_completion();

    // Get results
    auto results = scheduler->get_results();

    // Combine all result batches
    if (results.empty()) {
      env->ReleaseStringUTFChars(jpath, path);
      return 0;
    }

    // For now, return first batch
    // TODO: Concatenate all batches
    auto* batch_ptr = new std::shared_ptr<arrow::RecordBatch>(results[0]);

    env->ReleaseStringUTFChars(jpath, path);

    fprintf(stderr, "morsel: scanned %s -> %ld rows from %zu batches\n",
            path, results[0]->num_rows(), results.size());

    return reinterpret_cast<jlong>(batch_ptr);

  } catch (const std::exception& e) {
    fprintf(stderr, "morsel: error: %s\n", e.what());
    env->ReleaseStringUTFChars(jpath, path);
    return 0;
  }
}

// Get row count from batch
JNIEXPORT jint JNICALL
Java_org_apache_spark_sql_execution_MorselEngine_getBatchRows(
    JNIEnv*, jclass, jlong batch_handle) {

  auto* batch_ptr = reinterpret_cast<std::shared_ptr<arrow::RecordBatch>*>(batch_handle);
  return batch_ptr ? (*batch_ptr)->num_rows() : 0;
}

// Get column count
JNIEXPORT jint JNICALL
Java_org_apache_spark_sql_execution_MorselEngine_getBatchCols(
    JNIEnv*, jclass, jlong batch_handle) {

  auto* batch_ptr = reinterpret_cast<std::shared_ptr<arrow::RecordBatch>*>(batch_handle);
  return batch_ptr ? (*batch_ptr)->num_columns() : 0;
}

// Free batch
JNIEXPORT void JNICALL
Java_org_apache_spark_sql_execution_MorselEngine_freeBatch(
    JNIEnv*, jclass, jlong batch_handle) {

  auto* batch_ptr = reinterpret_cast<std::shared_ptr<arrow::RecordBatch>*>(batch_handle);
  delete batch_ptr;
}

// Shutdown scheduler
JNIEXPORT void JNICALL
Java_org_apache_spark_sql_execution_MorselEngine_shutdown(
    JNIEnv*, jclass, jlong scheduler_handle) {

  auto* scheduler = reinterpret_cast<MorselScheduler*>(scheduler_handle);
  scheduler->stop();
}

} // extern "C"
