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

package org.apache.spark.sql.execution.nativesql;

/**
 * JNI surface for {@code libspark_nativesql_jni}.
 *
 * <p>Each input is a columnar batch: {@code Object[]} of primitive arrays
 * ({@code int[]}, {@code long[]}, {@code double[]}, {@code boolean[]}).
 *
 * <p>Result is {@code Object[]{ Integer numRows, Object[] columns }}.
 */
public final class NativeSqlJni {
  private NativeSqlJni() {}

  public static native Object[] execute(String planIr, Object[][] columns, int[] numRows);
}
