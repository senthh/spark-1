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

import scala.jdk.CollectionConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnVector, ColumnarBatch}
import org.apache.spark.sql.vegam.plan.{HashAgg, NativePlan, NativePlanCodec}

/**
 * JNI backend. createTask / nextPage | nextBatch / close is the whole surface.
 * Stages run by Velox stream Arrow batches (exact types); the legacy fused
 * C++ pipeline still returns double pages.
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
    // Stay inside the task's CPU budget; YARN containers are sized by it.
    val threads = Option(TaskContext.get()).map(_.cpus()).getOrElse(1).max(1)
    val handle = NativeTask.createTask(NativePlanCodec.encode(plan), 0L, threads)
    if (handle == 0L) {
      throw SparkException.internalError("vegam createTask failed")
    }
    new NativeJniTask(handle, plan)
  }

  private class NativeJniTask(handle: Long, plan: NativePlan) extends VegamTask {
    private var closed = false
    private lazy val arrow = NativeTask.isArrow(handle)
    private var allocator: BufferAllocator = _
    private var root: VectorSchemaRoot = _

    override def isColumnar: Boolean = !closed && arrow

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

    override def nextBatch(): Option[ColumnarBatch] = {
      if (closed) {
        return None
      }
      releaseRoot()
      if (allocator == null) {
        allocator = ArrowUtils.rootAllocator.newChildAllocator(
          s"vegam-task-$handle", 0, Long.MaxValue)
      }
      val array = ArrowArray.allocateNew(allocator)
      val schema = ArrowSchema.allocateNew(allocator)
      try {
        val n = NativeTask.nextBatch(handle, array.memoryAddress(), schema.memoryAddress())
        if (n < 0) {
          None
        } else {
          root = Data.importVectorSchemaRoot(allocator, array, schema, null)
          val cols: Array[ColumnVector] =
            root.getFieldVectors.asScala.map(v => new ArrowColumnVector(v)).toArray
          Some(new ColumnarBatch(cols, root.getRowCount))
        }
      } finally {
        array.close()
        schema.close()
      }
    }

    override def close(): Unit = {
      if (!closed) {
        closed = true
        releaseRoot()
        NativeTask.close(handle)
        if (allocator != null) {
          allocator.close()
          allocator = null
        }
      }
    }

    private def releaseRoot(): Unit = {
      if (root != null) {
        root.close()
        root = null
      }
    }

    private def defaultCols: Int = plan match {
      case _: HashAgg => 2
      case _ => 1
    }
  }
}
