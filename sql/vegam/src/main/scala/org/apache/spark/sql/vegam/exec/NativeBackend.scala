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

package org.apache.spark.sql.vegam.exec

import org.apache.spark.SparkException
import org.apache.spark.sql.vegam.plan.{HashAgg, NativePlan, NativePlanCodec}

/**
 * JNI backend. createTask / nextPage / close is the whole surface.
 */
object NativeBackend extends VegamBackend {
  private val PAGE_CAP: Int = 65536
  private val META_COLS: Int = 0
  private val META_SCALE0: Int = 1
  private val META_SCALE1: Int = 2

  override def createTask(plan: NativePlan): VegamTask = {
    if (!NativeTask.isLoaded) {
      throw SparkException.internalError("vegam native backend requested but libvegam is missing")
    }
    val threads = math.max(1, Runtime.getRuntime.availableProcessors())
    val handle = NativeTask.createTask(NativePlanCodec.encode(plan), 0L, threads)
    if (handle == 0L) {
      throw SparkException.internalError("vegam createTask failed")
    }
    new NativeJniTask(handle, plan)
  }

  private class NativeJniTask(handle: Long, plan: NativePlan) extends VegamTask {
    private var closed = false

    override def nextPage(): Option[VegamPage] = {
      if (closed) {
        return None
      }
      val values = new Array[Double](PAGE_CAP * 2)
      val nulls = new Array[Boolean](PAGE_CAP * 2)
      val meta = new Array[Int](4)
      val n = NativeTask.nextPage(handle, values, nulls, meta)
      if (n < 0) {
        None
      } else {
        val cols = if (meta(META_COLS) > 0) meta(META_COLS) else defaultCols
        val scales = Array(meta(META_SCALE0), meta(META_SCALE1))
        Some(VegamPage(n, cols, values, nulls, scales))
      }
    }

    override def close(): Unit = {
      if (!closed) {
        closed = true
        NativeTask.close(handle)
      }
    }

    private def defaultCols: Int = plan match {
      case _: HashAgg => 2
      case _ => 1
    }
  }
}
