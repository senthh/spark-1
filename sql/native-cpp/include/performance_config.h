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

#ifndef SPARK_NATIVESQL_PERFORMANCE_CONFIG_H
#define SPARK_NATIVESQL_PERFORMANCE_CONFIG_H

/*
 * Velox-style Performance Configuration
 *
 * Key improvements:
 * 1. Larger batch sizes (10K→100K rows) to reduce JNI crossings
 * 2. SIMD vectorization via AVX2 instructions
 * 3. Memory pooling with 64-byte aligned allocations
 * 4. Aggressive compiler optimizations (-march=native -O3 -ffast-math)
 */

/* Default batch size: 100K rows (vs 10K default) - reduces JNI overhead */
#define NS_DEFAULT_BATCH_SIZE 100000

/* Arrow batch size for parquet reading - larger batches, fewer crossings */
#define NS_ARROW_BATCH_SIZE 100000

/* Memory pool size - 256MB per pool */
#define NS_MEMORY_POOL_SIZE (256 * 1024 * 1024)

/* Enable SIMD vectorization (compile with -march=native -mavx2) */
#define NS_USE_SIMD 1

/* Prefetch distance for sequential scans */
#define NS_PREFETCH_DISTANCE 8

/* Cache line size for alignment */
#define NS_CACHE_LINE_SIZE 64

#endif /* SPARK_NATIVESQL_PERFORMANCE_CONFIG_H */
