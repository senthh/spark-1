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

import java.net.URLDecoder
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration

object VegamPaths {

  def isPosix(uri: String): Boolean = {
    if (uri == null || uri.isEmpty) {
      false
    } else if (hasRemoteScheme(uri)) {
      false
    } else {
      uri.startsWith("/") || uri.startsWith("file:")
    }
  }

  /**
   * Hadoop FS paths we will read. HDFS/S3 go through the Java Hadoop client
   * (byte ranges). libhdfs dlopen and HDFS short-circuit are never used.
   */
  def isSupported(uri: String): Boolean = {
    if (uri == null || uri.isEmpty) {
      false
    } else {
      isPosix(uri) || hasRemoteScheme(uri)
    }
  }

  def clean(uri: String): String = {
    val decoded = try {
      URLDecoder.decode(uri, StandardCharsets.UTF_8)
    } catch {
      case _: Exception => uri
    }
    if (decoded.startsWith("file:")) {
      var i = 5
      while (i < decoded.length && decoded.charAt(i) == '/') {
        i += 1
      }
      "/" + decoded.substring(i)
    } else {
      decoded
    }
  }

  def hadoopConf(): Configuration = {
    val base = new Configuration()
    base.setBoolean("dfs.client.read.shortcircuit", false)
    base.set("dfs.client.read.shortcircuit", "false")
    base.setBoolean("dfs.client.use.legacy.blockreader.local", false)
    base
  }

  private def hasRemoteScheme(uri: String): Boolean = {
    uri.startsWith("hdfs://") ||
      uri.startsWith("viewfs://") ||
      uri.startsWith("s3a://") ||
      uri.startsWith("s3n://") ||
      uri.startsWith("s3://") ||
      uri.startsWith("abfs://") ||
      uri.startsWith("abfss://") ||
      uri.startsWith("gs://")
  }
}
