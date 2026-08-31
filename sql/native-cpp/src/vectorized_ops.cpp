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

#include "vectorized_ops.h"
#include <cstdlib>
#include <cstring>
#include <vector>

#if defined(__x86_64__) || defined(_M_X64)
#include <immintrin.h>
#define HAS_AVX2
#endif

#if defined(__aarch64__) || defined(_M_ARM64)
#include <arm_neon.h>
#define HAS_NEON
#endif

/* SIMD-vectorized filter for int64_t equality */
int64_t vectorized_filter_i64_eq(const int64_t *data, const uint8_t *valid,
                                  int64_t n, int64_t value, uint8_t *out_mask) {
  if (!data || !out_mask || n <= 0) return 0;

  int64_t count = 0;
  int64_t i = 0;

#ifdef HAS_AVX2
  if (n >= 4) {
    __m256i v_cmp = _mm256_set1_epi64x(value);
    for (; i + 4 <= n; i += 4) {
      __m256i v_data = _mm256_loadu_si256((__m256i*)(data + i));
      __m256i v_eq = _mm256_cmpeq_epi64(v_data, v_cmp);

      uint32_t mask = _mm256_movemask_pd(_mm256_castsi256_pd(v_eq));

      if (valid) {
        for (int j = 0; j < 4; j++) {
          uint8_t passes = ((mask >> j) & 1) && valid[i + j];
          out_mask[i + j] = passes;
          count += passes;
        }
      } else {
        for (int j = 0; j < 4; j++) {
          uint8_t passes = (mask >> j) & 1;
          out_mask[i + j] = passes;
          count += passes;
        }
      }
    }
  }
#endif

  /* Scalar tail */
  for (; i < n; i++) {
    uint8_t passes = (data[i] == value) && (!valid || valid[i]);
    out_mask[i] = passes;
    count += passes;
  }

  return count;
}

/* SIMD-vectorized filter for int64_t greater-than */
int64_t vectorized_filter_i64_gt(const int64_t *data, const uint8_t *valid,
                                  int64_t n, int64_t value, uint8_t *out_mask) {
  if (!data || !out_mask || n <= 0) return 0;

  int64_t count = 0;
  int64_t i = 0;

#ifdef HAS_AVX2
  if (n >= 4) {
    __m256i v_cmp = _mm256_set1_epi64x(value);
    for (; i + 4 <= n; i += 4) {
      __m256i v_data = _mm256_loadu_si256((__m256i*)(data + i));
      __m256i v_gt = _mm256_cmpgt_epi64(v_data, v_cmp);

      uint32_t mask = _mm256_movemask_pd(_mm256_castsi256_pd(v_gt));

      if (valid) {
        for (int j = 0; j < 4; j++) {
          uint8_t passes = ((mask >> j) & 1) && valid[i + j];
          out_mask[i + j] = passes;
          count += passes;
        }
      } else {
        for (int j = 0; j < 4; j++) {
          uint8_t passes = (mask >> j) & 1;
          out_mask[i + j] = passes;
          count += passes;
        }
      }
    }
  }
#endif

  /* Scalar tail */
  for (; i < n; i++) {
    uint8_t passes = (data[i] > value) && (!valid || valid[i]);
    out_mask[i] = passes;
    count += passes;
  }

  return count;
}

/* SIMD-vectorized filter for int64_t less-than */
int64_t vectorized_filter_i64_lt(const int64_t *data, const uint8_t *valid,
                                  int64_t n, int64_t value, uint8_t *out_mask) {
  if (!data || !out_mask || n <= 0) return 0;

  int64_t count = 0;
  int64_t i = 0;

#ifdef HAS_AVX2
  if (n >= 4) {
    __m256i v_cmp = _mm256_set1_epi64x(value);
    for (; i + 4 <= n; i += 4) {
      __m256i v_data = _mm256_loadu_si256((__m256i*)(data + i));
      __m256i v_gt = _mm256_cmpgt_epi64(v_cmp, v_data);  /* Note: reversed */

      uint32_t mask = _mm256_movemask_pd(_mm256_castsi256_pd(v_gt));

      if (valid) {
        for (int j = 0; j < 4; j++) {
          uint8_t passes = ((mask >> j) & 1) && valid[i + j];
          out_mask[i + j] = passes;
          count += passes;
        }
      } else {
        for (int j = 0; j < 4; j++) {
          uint8_t passes = (mask >> j) & 1;
          out_mask[i + j] = passes;
          count += passes;
        }
      }
    }
  }
#endif

  /* Scalar tail */
  for (; i < n; i++) {
    uint8_t passes = (data[i] < value) && (!valid || valid[i]);
    out_mask[i] = passes;
    count += passes;
  }

  return count;
}

/* GE and LE use GT/LT implementations */
int64_t vectorized_filter_i64_ge(const int64_t *data, const uint8_t *valid,
                                  int64_t n, int64_t value, uint8_t *out_mask) {
  return vectorized_filter_i64_lt(data, valid, n, value - 1, out_mask);
}

int64_t vectorized_filter_i64_le(const int64_t *data, const uint8_t *valid,
                                  int64_t n, int64_t value, uint8_t *out_mask) {
  return vectorized_filter_i64_gt(data, valid, n, value + 1, out_mask);
}

/* SIMD-vectorized filter for double greater-than */
int64_t vectorized_filter_f64_gt(const double *data, const uint8_t *valid,
                                  int64_t n, double value, uint8_t *out_mask) {
  if (!data || !out_mask || n <= 0) return 0;

  int64_t count = 0;
  int64_t i = 0;

#ifdef HAS_AVX2
  if (n >= 4) {
    __m256d v_cmp = _mm256_set1_pd(value);
    for (; i + 4 <= n; i += 4) {
      __m256d v_data = _mm256_loadu_pd(data + i);
      __m256d v_gt = _mm256_cmp_pd(v_data, v_cmp, _CMP_GT_OQ);

      uint32_t mask = _mm256_movemask_pd(v_gt);

      if (valid) {
        for (int j = 0; j < 4; j++) {
          uint8_t passes = ((mask >> j) & 1) && valid[i + j];
          out_mask[i + j] = passes;
          count += passes;
        }
      } else {
        for (int j = 0; j < 4; j++) {
          uint8_t passes = (mask >> j) & 1;
          out_mask[i + j] = passes;
          count += passes;
        }
      }
    }
  }
#endif

  /* Scalar tail */
  for (; i < n; i++) {
    uint8_t passes = (data[i] > value) && (!valid || valid[i]);
    out_mask[i] = passes;
    count += passes;
  }

  return count;
}

/* SIMD-vectorized filter for double less-than */
int64_t vectorized_filter_f64_lt(const double *data, const uint8_t *valid,
                                  int64_t n, double value, uint8_t *out_mask) {
  if (!data || !out_mask || n <= 0) return 0;

  int64_t count = 0;
  int64_t i = 0;

#ifdef HAS_AVX2
  if (n >= 4) {
    __m256d v_cmp = _mm256_set1_pd(value);
    for (; i + 4 <= n; i += 4) {
      __m256d v_data = _mm256_loadu_pd(data + i);
      __m256d v_lt = _mm256_cmp_pd(v_data, v_cmp, _CMP_LT_OQ);

      uint32_t mask = _mm256_movemask_pd(v_lt);

      if (valid) {
        for (int j = 0; j < 4; j++) {
          uint8_t passes = ((mask >> j) & 1) && valid[i + j];
          out_mask[i + j] = passes;
          count += passes;
        }
      } else {
        for (int j = 0; j < 4; j++) {
          uint8_t passes = (mask >> j) & 1;
          out_mask[i + j] = passes;
          count += passes;
        }
      }
    }
  }
#endif

  /* Scalar tail */
  for (; i < n; i++) {
    uint8_t passes = (data[i] < value) && (!valid || valid[i]);
    out_mask[i] = passes;
    count += passes;
  }

  return count;
}

/* SIMD-vectorized int64 addition */
void vectorized_add_i64(const int64_t *a, const int64_t *b, int64_t n, int64_t *out) {
  if (!a || !b || !out || n <= 0) return;

  int64_t i = 0;

#ifdef HAS_AVX2
  for (; i + 4 <= n; i += 4) {
    __m256i va = _mm256_loadu_si256((__m256i*)(a + i));
    __m256i vb = _mm256_loadu_si256((__m256i*)(b + i));
    __m256i vr = _mm256_add_epi64(va, vb);
    _mm256_storeu_si256((__m256i*)(out + i), vr);
  }
#endif

  for (; i < n; i++) {
    out[i] = a[i] + b[i];
  }
}

/* SIMD-vectorized double addition */
void vectorized_add_f64(const double *a, const double *b, int64_t n, double *out) {
  if (!a || !b || !out || n <= 0) return;

  int64_t i = 0;

#ifdef HAS_AVX2
  for (; i + 4 <= n; i += 4) {
    __m256d va = _mm256_loadu_pd(a + i);
    __m256d vb = _mm256_loadu_pd(b + i);
    __m256d vr = _mm256_add_pd(va, vb);
    _mm256_storeu_pd(out + i, vr);
  }
#endif

  for (; i < n; i++) {
    out[i] = a[i] + b[i];
  }
}

/* SIMD-vectorized int64 multiplication */
void vectorized_mul_i64(const int64_t *a, const int64_t *b, int64_t n, int64_t *out) {
  if (!a || !b || !out || n <= 0) return;

  /* Note: AVX2 doesn't have _mm256_mul_epi64, so we use scalar for now */
  for (int64_t i = 0; i < n; i++) {
    out[i] = a[i] * b[i];
  }
}

/* SIMD-vectorized double multiplication */
void vectorized_mul_f64(const double *a, const double *b, int64_t n, double *out) {
  if (!a || !b || !out || n <= 0) return;

  int64_t i = 0;

#ifdef HAS_AVX2
  for (; i + 4 <= n; i += 4) {
    __m256d va = _mm256_loadu_pd(a + i);
    __m256d vb = _mm256_loadu_pd(b + i);
    __m256d vr = _mm256_mul_pd(va, vb);
    _mm256_storeu_pd(out + i, vr);
  }
#endif

  for (; i < n; i++) {
    out[i] = a[i] * b[i];
  }
}

/* Memory pool implementation */
struct MemoryPool {
  std::vector<char*> pools;
  size_t pool_size;
  size_t current_offset;
  int current_pool_idx;

  MemoryPool(size_t ps) : pool_size(ps), current_offset(0), current_pool_idx(-1) {}

  ~MemoryPool() {
    for (char* p : pools) {
      free(p);
    }
  }
};

MemoryPool *memory_pool_create(size_t pool_size) {
  return new MemoryPool(pool_size);
}

void *memory_pool_allocate(MemoryPool *pool, size_t size) {
  if (!pool || size == 0) return nullptr;

  /* Align to 64 bytes for SIMD */
  size_t aligned_size = (size + 63) & ~63ULL;

  /* Check if we need a new pool */
  if (pool->current_pool_idx < 0 ||
      pool->current_offset + aligned_size > pool->pool_size) {
    char *new_pool = (char*)aligned_alloc(64, pool->pool_size);
    if (!new_pool) return nullptr;
    pool->pools.push_back(new_pool);
    pool->current_pool_idx = pool->pools.size() - 1;
    pool->current_offset = 0;
  }

  char *ptr = pool->pools[pool->current_pool_idx] + pool->current_offset;
  pool->current_offset += aligned_size;

  return ptr;
}

void memory_pool_reset(MemoryPool *pool) {
  if (!pool) return;
  pool->current_offset = 0;
  pool->current_pool_idx = pool->pools.empty() ? -1 : 0;
}

void memory_pool_destroy(MemoryPool *pool) {
  delete pool;
}
