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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Fused scan + optional greater-than filter + group-by sum, executed in C++.
 */
case class MorselHashAggExec(
    filePath: String,
    groupCol: String,
    sumCol: String,
    filterCol: Option[String],
    filterValue: Long,
    output: Seq[Attribute])
  extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val path = MorselPaths.clean(filePath)
    val group = groupCol
    val sum = sumCol
    val fcol = filterCol.orNull
    val fval = filterValue
    val outSchema = output.map(_.dataType).toArray
    val schema = output
    sparkContext.parallelize(Seq(path), 1).mapPartitions { iter =>
      if (!iter.hasNext) {
        Iterator.empty
      } else {
        val scheduler = MorselEngine.initScheduler(8)
        val handle = MorselEngine.hashAggregate(
          scheduler, iter.next(), group, sum, fcol, fval)
        if (handle == 0) {
          Iterator.empty
        } else {
          try {
            val n = MorselEngine.getAggRows(handle)
            val keys = new Array[Long](n)
            val sums = new Array[Double](n)
            MorselEngine.copyAggKeys(handle, keys)
            MorselEngine.copyAggSums(handle, sums)
            val proj = UnsafeProjection.create(schema.map(_.dataType).toArray)
            (0 until n).iterator.map { i =>
              val row = new GenericInternalRow(outSchema.length)
              if (outSchema.length >= 1) {
                row.update(0, MorselHashAggExec.asSpark(keys(i).toDouble, outSchema(0)))
              }
              if (outSchema.length >= 2) {
                row.update(1, MorselHashAggExec.asSpark(sums(i), outSchema(1)))
              }
              proj(row).copy()
            }
          } finally {
            MorselEngine.freeAgg(handle)
          }
        }
      }
    }
  }
}

object MorselHashAggExec {
  def asSpark(value: Double, dt: DataType): Any = dt match {
    case IntegerType => value.toInt
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
