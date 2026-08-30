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

#ifndef SPARK_NATIVESQL_SORT_H
#define SPARK_NATIVESQL_SORT_H

#include "nativesql.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Fill perm[0 .. n_rows) with an ascending permutation of row indices.
 * Sort by keys[0], then refine ties with keys[1], keys[2], ...
 *
 * Integer keys (NS_I32 / NS_I64) use LSD radix sort on (key, index) pairs
 * when a range has at least 256 rows; smaller ranges and float/bool keys
 * use std::sort on the permutation. trySort skips a full sort when a
 * range is already ordered or has a single adjacent inversion.
 *
 * Returns 0 on success, -1 on bad arguments.
 */
int ns_sort_permutation(const NsCol *keys, int n_keys, int32_t n_rows, uint32_t *perm);

#ifdef __cplusplus
}
#endif

#endif /* SPARK_NATIVESQL_SORT_H */
