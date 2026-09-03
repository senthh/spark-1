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

package org.apache.spark.sql.execution.morsel

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}

/**
 * Native parquet scan that reports how many rows the file contributes without
 * handing any column values back to the JVM.
 *
 * Because it produces no column values, it can only stand in for a scan whose
 * output is empty. MorselColumnarRule enforces that.
 */
case class MorselScanExec(
    filePath: String,
    output: Seq[Attribute],
    filterCol: Int = -1,
    filterValue: Long = 0)
  extends LeafExecNode {

  // Rows leave the native side in batch units. Emitting one InternalRow per
  // scanned row put millions of iterator steps on the critical path of a
  // COUNT(*), which is work Spark's own vectorized reader never does.
  override def supportsColumnar: Boolean = true

  private def scannedRows(): RDD[Long] = {
    val columnNames = output.map(_.name).toArray
    val fCol = filterCol
    val fVal = filterValue
    sparkContext.parallelize(Seq(MorselPaths.clean(filePath)), 1).map { path =>
      MorselScanExec.scanRowCount(path, columnNames, fCol, fVal)
    }
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    scannedRows().mapPartitions { iter =>
      if (iter.hasNext) MorselScanExec.emptyBatches(iter.next()) else Iterator.empty
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    scannedRows().mapPartitions { iter =>
      if (iter.hasNext) MorselScanExec.emptyRows(iter.next()) else Iterator.empty
    }
  }
}

object MorselScanExec {
  private val NumThreads = 8
  private val BatchSize = 4096

  private def scanRowCount(
      path: String,
      columns: Array[String],
      filterCol: Int,
      filterValue: Long): Long = {
    val scheduler = MorselEngine.initScheduler(NumThreads)
    try {
      val handle = MorselEngine.scanParquet(scheduler, path, columns, filterCol, filterValue)
      if (handle == 0) {
        0L
      } else {
        try {
          MorselEngine.getBatchRows(handle)
        } finally {
          MorselEngine.freeBatch(handle)
        }
      }
    } finally {
      MorselEngine.shutdown(scheduler)
    }
  }

  /**
   * Batches carrying a row count and no columns, which is the same shape
   * Spark's vectorized reader produces for a COUNT(*). The batch is reused
   * across calls, so callers must consume one before asking for the next.
   */
  private def emptyBatches(totalRows: Long): Iterator[ColumnarBatch] = {
    val batch = new ColumnarBatch(Array.empty[ColumnVector], 0)
    Option(TaskContext.get()).foreach { tc =>
      tc.addTaskCompletionListener[Unit](_ => batch.close())
    }
    new Iterator[ColumnarBatch] {
      private var remaining = totalRows
      override def hasNext: Boolean = remaining > 0
      override def next(): ColumnarBatch = {
        val n = math.min(BatchSize.toLong, remaining).toInt
        remaining -= n
        batch.setNumRows(n)
        batch
      }
    }
  }

  private def emptyRows(totalRows: Long): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      private var remaining = totalRows
      override def hasNext: Boolean = remaining > 0
      override def next(): InternalRow = {
        remaining -= 1
        InternalRow.empty
      }
    }
  }
}
