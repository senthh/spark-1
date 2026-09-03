/*
 * JNI Bridge for Morsel-Driven Execution
 * Minimal JNI crossings with large result batches
 */

#include "morsel_scheduler.h"
#include "morsel_operators.h"
#include <jni.h>
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

using namespace morsel;

struct MorselScanResult {
  int64_t num_rows = 0;
  int num_cols = 0;
};

struct MorselAggResult {
  std::vector<int64_t> keys;
  std::vector<double> sums;
};

// One scheduler per executor thread that first called init.
static thread_local std::unique_ptr<MorselScheduler> g_scheduler;

static std::string clean_fs_path(const char* raw) {
  std::string p = raw ? raw : "";
  if (p.compare(0, 5, "file:") == 0) {
    size_t i = 5;
    while (i < p.size() && p[i] == '/') {
      i++;
    }
    p = "/" + p.substr(i);
  } else if (p.compare(0, 7, "hdfs://") == 0) {
    auto slash = p.find('/', 7);
    if (slash != std::string::npos) {
      p = p.substr(slash);
    }
  }
  if (!p.empty() && p[0] != '/') {
    p = "/" + p;
  }
  return p;
}

extern "C" {

JNIEXPORT jlong JNICALL
Java_org_apache_spark_sql_execution_morsel_MorselEngine_initScheduler(
    JNIEnv*, jclass, jint num_threads) {
  if (!g_scheduler) {
    g_scheduler = std::make_unique<MorselScheduler>(num_threads);
  }
  return reinterpret_cast<jlong>(g_scheduler.get());
}

JNIEXPORT jlong JNICALL
Java_org_apache_spark_sql_execution_morsel_MorselEngine_scanParquet(
    JNIEnv* env,
    jclass,
    jlong scheduler_handle,
    jstring jpath,
    jobjectArray jcolumns,
    jint jfilter_col,
    jlong jfilter_value) {

  auto* scheduler = reinterpret_cast<MorselScheduler*>(scheduler_handle);
  const char* raw_path = env->GetStringUTFChars(jpath, nullptr);
  std::string path = clean_fs_path(raw_path);

  std::vector<std::string> columns;
  jsize num_cols = jcolumns ? env->GetArrayLength(jcolumns) : 0;
  for (jsize i = 0; i < num_cols; i++) {
    auto jcol = reinterpret_cast<jstring>(env->GetObjectArrayElement(jcolumns, i));
    const char* col = env->GetStringUTFChars(jcol, nullptr);
    columns.push_back(col);
    env->ReleaseStringUTFChars(jcol, col);
  }

  try {
    if (!scheduler) {
      throw std::runtime_error("null scheduler");
    }
    // COUNT(*) with no filter: footer metadata, no decode.
    if (columns.empty() && jfilter_col < 0) {
      auto* out = new MorselScanResult();
      out->num_rows = parquet_footer_rows(path);
      out->num_cols = 0;
      env->ReleaseStringUTFChars(jpath, raw_path);
      std::fprintf(stderr, "morsel: footer %s rows=%ld\n",
                   path.c_str(), static_cast<long>(out->num_rows));
      std::fflush(stderr);
      return reinterpret_cast<jlong>(out);
    }
    scheduler->reset();

    auto pipeline = std::make_shared<Pipeline>();
    auto scan_op = std::make_shared<ParquetScanOperator>(path, columns);
    const int nrg = scan_op->row_groups();
    pipeline->add_operator(scan_op);

    if (jfilter_col >= 0) {
      auto filter_literal = std::make_shared<arrow::Int64Scalar>(jfilter_value);
      auto filter_op = std::make_shared<FilterOperator>(
        jfilter_col,
        arrow::compute::CompareOperator::GREATER,
        filter_literal);
      pipeline->add_operator(filter_op);
    }

    scheduler->add_pipeline(pipeline);
    // One token per row group. morsel_id is the row-group index.
    scheduler->schedule_row_groups(0, nrg);
    scheduler->start();
    scheduler->wait_for_completion();

    auto* out = new MorselScanResult();
    out->num_rows = scan_op->total_rows_read();
    auto results = scheduler->get_results();
    if (!results.empty() && results[0]) {
      out->num_cols = results[0]->num_columns();
    }

    // Workers never ran or every ReadRowGroup failed: read on this thread.
    if (out->num_rows == 0 && nrg > 0) {
      std::fprintf(stderr,
                   "morsel: parallel scan produced 0 rows (err=%s); sequential fallback\n",
                   scheduler->error_message().c_str());
      std::fflush(stderr);
      for (int rg = 0; rg < nrg; rg++) {
        scan_op->read_row_group(rg);
      }
      out->num_rows = scan_op->total_rows_read();
    }

    env->ReleaseStringUTFChars(jpath, raw_path);
    std::fprintf(stderr, "morsel: scanned %s rgs=%d rows=%ld batches=%zu\n",
                 path.c_str(), nrg, static_cast<long>(out->num_rows), results.size());
    std::fflush(stderr);
    return reinterpret_cast<jlong>(out);

  } catch (const std::exception& e) {
    std::fprintf(stderr, "morsel: error path=%s: %s\n",
                 path.c_str(), e.what());
    std::fflush(stderr);
    env->ReleaseStringUTFChars(jpath, raw_path);
    return 0;
  }
}

JNIEXPORT jlong JNICALL
Java_org_apache_spark_sql_execution_morsel_MorselEngine_getBatchRows(
    JNIEnv*, jclass, jlong batch_handle) {
  auto* r = reinterpret_cast<MorselScanResult*>(batch_handle);
  return r ? static_cast<jlong>(r->num_rows) : 0;
}

JNIEXPORT jint JNICALL
Java_org_apache_spark_sql_execution_morsel_MorselEngine_getBatchCols(
    JNIEnv*, jclass, jlong batch_handle) {
  auto* r = reinterpret_cast<MorselScanResult*>(batch_handle);
  return r ? r->num_cols : 0;
}

JNIEXPORT void JNICALL
Java_org_apache_spark_sql_execution_morsel_MorselEngine_freeBatch(
    JNIEnv*, jclass, jlong batch_handle) {
  delete reinterpret_cast<MorselScanResult*>(batch_handle);
}

JNIEXPORT void JNICALL
Java_org_apache_spark_sql_execution_morsel_MorselEngine_shutdown(
    JNIEnv*, jclass, jlong scheduler_handle) {
  auto* scheduler = reinterpret_cast<MorselScheduler*>(scheduler_handle);
  if (scheduler) {
    scheduler->stop();
  }
}

JNIEXPORT jlong JNICALL
Java_org_apache_spark_sql_execution_morsel_MorselEngine_footerRowCount(
    JNIEnv* env, jclass, jstring jpath) {
  const char* raw_path = env->GetStringUTFChars(jpath, nullptr);
  std::string path = clean_fs_path(raw_path);
  try {
    const int64_t n = parquet_footer_rows(path);
    env->ReleaseStringUTFChars(jpath, raw_path);
    std::fprintf(stderr, "morsel: footer %s rows=%ld\n",
                 path.c_str(), static_cast<long>(n));
    std::fflush(stderr);
    return n;
  } catch (const std::exception& e) {
    std::fprintf(stderr, "morsel: footer error path=%s: %s\n",
                 path.c_str(), e.what());
    std::fflush(stderr);
    env->ReleaseStringUTFChars(jpath, raw_path);
    return -1;
  }
}

JNIEXPORT jlong JNICALL
Java_org_apache_spark_sql_execution_morsel_MorselEngine_hashAggregate(
    JNIEnv* env,
    jclass,
    jlong scheduler_handle,
    jstring jpath,
    jstring jgroup,
    jstring jsum,
    jstring jfilter,
    jlong jfilter_value) {

  auto* scheduler = reinterpret_cast<MorselScheduler*>(scheduler_handle);
  const char* raw_path = env->GetStringUTFChars(jpath, nullptr);
  std::string path = clean_fs_path(raw_path);
  const char* group_c = env->GetStringUTFChars(jgroup, nullptr);
  const char* sum_c = env->GetStringUTFChars(jsum, nullptr);
  const char* filter_c = jfilter ? env->GetStringUTFChars(jfilter, nullptr) : nullptr;
  std::string group = group_c ? group_c : "";
  std::string sum = sum_c ? sum_c : "";
  std::string filter = filter_c ? filter_c : "";

  try {
    if (!scheduler) {
      throw std::runtime_error("null scheduler");
    }
    if (group.empty() || sum.empty()) {
      throw std::runtime_error("group/sum column required");
    }

    std::vector<std::string> cols;
    cols.push_back(group);
    if (sum != group) {
      cols.push_back(sum);
    }
    if (!filter.empty() && filter != group && filter != sum) {
      cols.push_back(filter);
    }

    int group_idx = 0;
    int sum_idx = (sum == group) ? 0 : 1;
    int filter_idx = -1;
    if (!filter.empty()) {
      for (size_t i = 0; i < cols.size(); i++) {
        if (cols[i] == filter) {
          filter_idx = static_cast<int>(i);
        }
      }
    }

    scheduler->reset();
    auto pipeline = std::make_shared<Pipeline>();
    pipeline->requires_finalize = true;
    auto scan_op = std::make_shared<ParquetScanOperator>(path, cols);
    const int nrg = scan_op->row_groups();
    pipeline->add_operator(scan_op);

    if (filter_idx >= 0) {
      auto filter_literal = std::make_shared<arrow::Int64Scalar>(jfilter_value);
      pipeline->add_operator(std::make_shared<FilterOperator>(
          filter_idx, arrow::compute::CompareOperator::GREATER, filter_literal));
    }

    auto agg_op = std::make_shared<HashAggregateOperator>(
        std::vector<int>{group_idx},
        std::vector<int>{sum_idx},
        std::vector<std::string>{"sum"},
        64);
    pipeline->add_operator(agg_op);

    scheduler->add_pipeline(pipeline);
    scheduler->schedule_row_groups(0, nrg);
    scheduler->start();
    scheduler->wait_for_completion();

    ExecutionContext ctx(0, 0);
    auto batch = agg_op->finalize(&ctx);
    auto* out = new MorselAggResult();
    if (batch && batch->num_rows() > 0) {
      auto keys = std::static_pointer_cast<arrow::Int64Array>(batch->column(0));
      auto sums = std::static_pointer_cast<arrow::DoubleArray>(batch->column(1));
      out->keys.resize(static_cast<size_t>(batch->num_rows()));
      out->sums.resize(static_cast<size_t>(batch->num_rows()));
      for (int64_t i = 0; i < batch->num_rows(); i++) {
        out->keys[static_cast<size_t>(i)] = keys->Value(i);
        out->sums[static_cast<size_t>(i)] = sums->Value(i);
      }
    }

    env->ReleaseStringUTFChars(jpath, raw_path);
    env->ReleaseStringUTFChars(jgroup, group_c);
    env->ReleaseStringUTFChars(jsum, sum_c);
    if (filter_c) {
      env->ReleaseStringUTFChars(jfilter, filter_c);
    }
    std::fprintf(stderr,
                 "morsel: hashagg %s groups=%zu rgs=%d filter=%s>%ld\n",
                 path.c_str(), out->keys.size(), nrg, filter.c_str(),
                 static_cast<long>(jfilter_value));
    std::fflush(stderr);
    return reinterpret_cast<jlong>(out);

  } catch (const std::exception& e) {
    std::fprintf(stderr, "morsel: hashagg error path=%s: %s\n",
                 path.c_str(), e.what());
    std::fflush(stderr);
    env->ReleaseStringUTFChars(jpath, raw_path);
    env->ReleaseStringUTFChars(jgroup, group_c);
    env->ReleaseStringUTFChars(jsum, sum_c);
    if (filter_c) {
      env->ReleaseStringUTFChars(jfilter, filter_c);
    }
    return 0;
  }
}

JNIEXPORT jint JNICALL
Java_org_apache_spark_sql_execution_morsel_MorselEngine_getAggRows(
    JNIEnv*, jclass, jlong handle) {
  auto* r = reinterpret_cast<MorselAggResult*>(handle);
  return r ? static_cast<jint>(r->keys.size()) : 0;
}

JNIEXPORT void JNICALL
Java_org_apache_spark_sql_execution_morsel_MorselEngine_copyAggKeys(
    JNIEnv* env, jclass, jlong handle, jlongArray dest) {
  auto* r = reinterpret_cast<MorselAggResult*>(handle);
  if (!r || !dest) return;
  const jsize n = env->GetArrayLength(dest);
  const jsize copy = std::min(n, static_cast<jsize>(r->keys.size()));
  if (copy > 0) {
    env->SetLongArrayRegion(dest, 0, copy, reinterpret_cast<const jlong*>(r->keys.data()));
  }
}

JNIEXPORT void JNICALL
Java_org_apache_spark_sql_execution_morsel_MorselEngine_copyAggSums(
    JNIEnv* env, jclass, jlong handle, jdoubleArray dest) {
  auto* r = reinterpret_cast<MorselAggResult*>(handle);
  if (!r || !dest) return;
  const jsize n = env->GetArrayLength(dest);
  const jsize copy = std::min(n, static_cast<jsize>(r->sums.size()));
  if (copy > 0) {
    env->SetDoubleArrayRegion(dest, 0, copy, r->sums.data());
  }
}

JNIEXPORT void JNICALL
Java_org_apache_spark_sql_execution_morsel_MorselEngine_freeAgg(
    JNIEnv*, jclass, jlong handle) {
  delete reinterpret_cast<MorselAggResult*>(handle);
}

} // extern "C"
