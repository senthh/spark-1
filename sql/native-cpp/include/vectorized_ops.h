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

#ifndef SPARK_NATIVESQL_VECTORIZED_OPS_H
#define SPARK_NATIVESQL_VECTORIZED_OPS_H

#include <cstdint>
#include <cstddef>

#ifdef __cplusplus
extern "C" {
#endif

/* SIMD-vectorized filter operations (returns number of rows passing filter) */
int64_t vectorized_filter_i64_eq(const int64_t *data, const uint8_t *valid,
                                  int64_t n, int64_t value, uint8_t *out_mask);
int64_t vectorized_filter_i64_gt(const int64_t *data, const uint8_t *valid,
                                  int64_t n, int64_t value, uint8_t *out_mask);
int64_t vectorized_filter_i64_lt(const int64_t *data, const uint8_t *valid,
                                  int64_t n, int64_t value, uint8_t *out_mask);
int64_t vectorized_filter_i64_ge(const int64_t *data, const uint8_t *valid,
                                  int64_t n, int64_t value, uint8_t *out_mask);
int64_t vectorized_filter_i64_le(const int64_t *data, const uint8_t *valid,
                                  int64_t n, int64_t value, uint8_t *out_mask);

int64_t vectorized_filter_f64_gt(const double *data, const uint8_t *valid,
                                  int64_t n, double value, uint8_t *out_mask);
int64_t vectorized_filter_f64_lt(const double *data, const uint8_t *valid,
                                  int64_t n, double value, uint8_t *out_mask);

/* SIMD-vectorized arithmetic operations */
void vectorized_add_i64(const int64_t *a, const int64_t *b, int64_t n, int64_t *out);
void vectorized_add_f64(const double *a, const double *b, int64_t n, double *out);
void vectorized_mul_i64(const int64_t *a, const int64_t *b, int64_t n, int64_t *out);
void vectorized_mul_f64(const double *a, const double *b, int64_t n, double *out);

/* Memory pool for batch allocations */
typedef struct MemoryPool MemoryPool;

MemoryPool *memory_pool_create(size_t pool_size);
void *memory_pool_allocate(MemoryPool *pool, size_t size);
void memory_pool_reset(MemoryPool *pool);
void memory_pool_destroy(MemoryPool *pool);

#ifdef __cplusplus
}
#endif

#endif /* SPARK_NATIVESQL_VECTORIZED_OPS_H */
