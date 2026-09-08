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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types._
import org.apache.spark.sql.vegam.exec.{VegamBackend, VegamPage}
import org.apache.spark.sql.vegam.plan.{CountStar, HashAgg, NativePlan, StagePlan}
import org.apache.spark.unsafe.types.UTF8String

/**
 * The only SparkPlan leaf that talks to a Vegam backend. One convert at the
 * stage edge: backend pages become UnsafeRows.
 */
case class NativeStageExec(
    nativePlan: NativePlan,
    output: Seq[Attribute],
    backendName: String)
  extends LeafExecNode {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def simpleString(maxFields: Int): String = {
    s"NativeStageExec($backendName, ${nativePlan.getClass.getSimpleName})"
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val plan = nativePlan
    val backend = backendName
    val schema = output
    val splits = NativeStageExec.splits(plan)
    val numOutputRows = longMetric("numOutputRows")
    sparkContext.parallelize(splits, math.max(splits.length, 1)).mapPartitions { iter =>
      if (!iter.hasNext) {
        Iterator.empty
      } else {
        val files = iter.next()
        val taskPlan = plan.withFiles(files)
        val task = VegamBackend.create(backend).createTask(taskPlan)
        // Materialize before close: pages(task) is lazy, and try/finally would
        // close the task before Spark consumed the iterator.
        try {
          val proj = UnsafeProjection.create(schema.map(_.dataType).toArray)
          val rows = NativeStageExec.pages(task).flatMap { page =>
            (0 until page.numRows).iterator.map { i =>
              proj(NativeStageExec.toRow(page, i, schema)).copy()
            }
          }.toArray
          numOutputRows += rows.length
          rows.iterator
        } finally {
          task.close()
        }
      }
    }
  }
}

object NativeStageExec {
  private def splits(plan: NativePlan): Seq[Seq[String]] = plan match {
    case CountStar(files) => Seq(files)
    case h: HashAgg if h.complete => Seq(h.files)
    case h: HashAgg => h.files.map(f => Seq(f))
    case s: StagePlan if s.complete || s.window.isDefined => Seq(s.files)
    case s: StagePlan => s.files.map(f => Seq(f))
    case other => Seq(other.files)
  }

  private def pages(task: exec.VegamTask): Iterator[VegamPage] = {
    Iterator.continually(task.nextPage()).takeWhile(_.isDefined).map(_.get)
  }

  private def toRow(page: VegamPage, i: Int, schema: Seq[Attribute]): GenericInternalRow = {
    val row = new GenericInternalRow(schema.length)
    var c = 0
    while (c < schema.length && c < page.numCols) {
      if (page.isNull(i, c)) {
        row.setNullAt(c)
      } else if (page.isText(i, c)) {
        row.update(c, UTF8String.fromString(page.getText(i, c)))
      } else {
        row.update(c, asSpark(page.getDouble(i, c), schema(c).dataType, page.sumScale(c)))
      }
      c += 1
    }
    row
  }

  def asSpark(value: Double, dt: DataType, unscaledScale: Int = 0): Any = dt match {
    case IntegerType => value.toInt
    case LongType if unscaledScale > 0 =>
      math.round(value * math.pow(10.0, unscaledScale.toDouble))
    case LongType => value.toLong
    case FloatType => value.toFloat
    case DoubleType => value
    case d: DecimalType =>
      Decimal(BigDecimal(value), d.precision, d.scale)
    case StringType =>
      UTF8String.fromString(java.lang.Long.toString(value.toLong))
    case _ => value
  }
}
