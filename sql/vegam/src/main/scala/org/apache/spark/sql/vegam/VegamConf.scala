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

package org.apache.spark.sql.vegam

import org.apache.spark.sql.internal.SQLConf

/**
 * Configuration for the Vegam native engine. Loading this object registers
 * the entries with [[SQLConf]].
 */
object VegamConf {

  val VEGAM_ENABLED = SQLConf.buildConf("spark.sql.vegam.enabled")
    .doc("When true, rewrite fully-supported Spark stages as a Vegam " +
      "NativeStage. Default is false.")
    .version("4.2.0")
    .booleanConf
    .createWithDefault(false)

  val VEGAM_BACKEND = SQLConf.buildConf("spark.sql.vegam.backend")
    .doc("Vegam execution backend: auto (native JNI if libvegam is loaded, " +
      "otherwise the IR interpreter), native, or jvm.")
    .version("4.2.0")
    .stringConf
    .createWithDefault("auto")

  def enabled(conf: SQLConf): Boolean = conf.getConf(VEGAM_ENABLED)

  def backend(conf: SQLConf): String = {
    conf.getConf(VEGAM_BACKEND).toLowerCase(java.util.Locale.ROOT)
  }
}
