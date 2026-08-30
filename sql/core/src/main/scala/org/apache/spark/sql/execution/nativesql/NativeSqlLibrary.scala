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

package org.apache.spark.sql.execution.nativesql

import java.io.File

import org.apache.spark.SparkFiles
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

/**
 * Loads `libspark_nativesql_jni` and forwards plan IR + columnar batches to C++.
 *
 * Column encoding: each column is a primitive Java array (int[] / long[] / double[] / boolean[]).
 * Result is Object[]{ int numRows, Object[] columns } where columns use the same encoding.
 */
object NativeSqlLibrary extends Logging {

  @volatile private var loaded = false
  @volatile private var loadError: Option[Throwable] = None

  def isAvailable: Boolean = {
    ensureLoaded()
    loaded
  }

  def execute(planIr: String, columns: Array[Array[AnyRef]], numRows: Array[Int]): Array[AnyRef] = {
    ensureLoaded()
    if (!loaded) {
      throw loadError.getOrElse(new IllegalStateException("Native SQL library is not loaded"))
    }
    NativeSqlJni.execute(planIr, columns, numRows)
  }

  private def ensureLoaded(): Unit = synchronized {
    if (loaded) return
    val explicit = SQLConf.get.nativeSqlLib.filter(_.nonEmpty)
    // Retry after a failed default loadLibrary if an explicit path is set later.
    if (loadError.isDefined && explicit.isEmpty) return
    loadError = None
    try {
      explicit match {
        case Some(path) =>
          val name = new File(path).getName
          val sparkFile = try {
            Some(new File(SparkFiles.get(name)))
          } catch {
            case _: Throwable => None
          }
          val candidates = Seq(new File(path), new File(name)) ++ sparkFile.toSeq
          val resolved = candidates.find(_.isFile).getOrElse {
            throw new IllegalArgumentException(s"spark.sql.nativesql.lib does not exist: $path")
          }
          System.load(resolved.getAbsolutePath)
        case None =>
          System.loadLibrary("spark_nativesql_jni")
      }
      loaded = true
      logInfo("Loaded Native SQL JNI library")
    } catch {
      case t: Throwable =>
        loadError = Some(t)
        logWarning(s"Native SQL library not loaded: ${t.getMessage}")
    }
  }
}
