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

#ifndef SPARK_NATIVESQL_H
#define SPARK_NATIVESQL_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum NsType {
  NS_I32 = 1,
  NS_I64 = 2,
  NS_F64 = 3,
  NS_BOOL = 4
} NsType;

typedef struct NsCol {
  NsType type;
  int32_t n_rows;
  void *data; /* int32_t* / int64_t* / double* / uint8_t* */
} NsCol;

typedef struct NsBatch {
  int32_t n_cols;
  int32_t n_rows;
  NsCol *cols;
} NsBatch;

/* Execute IR. inputs[i] may be empty when the leaf is a generated range. */
int ns_execute(const char *plan_ir, const NsBatch *inputs, int n_inputs, NsBatch *out);

void ns_batch_free(NsBatch *b);

#ifdef __cplusplus
}
#endif

#endif /* SPARK_NATIVESQL_H */
