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

#ifndef SPARK_NATIVESQL_PARQUET_SCAN_H
#define SPARK_NATIVESQL_PARQUET_SCAN_H

#include "nativesql.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Read parquet splits into an NsBatch (malloc'd columns). Returns 0 on success. */
int ns_parquet_read(const NsFileScan *scan, NsBatch *out);

/* Optional Hadoop FS callbacks (JNI). When set, hdfs:// opens use these. */
typedef int64_t (*NsHdfsSizeFn)(const char *uri);
typedef int64_t (*NsHdfsPreadFn)(const char *uri, int64_t off, void *buf, int64_t n);
void ns_parquet_set_hdfs_io(NsHdfsSizeFn size_fn, NsHdfsPreadFn pread_fn);

#ifdef __cplusplus
}
#endif

#endif
