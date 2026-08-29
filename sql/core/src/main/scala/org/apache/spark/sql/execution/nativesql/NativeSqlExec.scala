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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.LeafExecNode
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

/**
 * Physical node that evaluates a Native SQL IR tree via JNI.
 * Inputs are local (LocalRelation / Range); the C++ engine runs the full subtree.
 */
case class NativeSqlExec(compiled: NativeSqlPlan.Compiled) extends LeafExecNode {

  override def output: Seq[Attribute] = compiled.output

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  @transient private lazy val resultRows: Array[InternalRow] = {
    val (batches, numRows) = encodeLeaves(compiled.leaves)
    val raw = NativeSqlLibrary.execute(compiled.ir, batches, numRows)
    val outRows = raw(0).asInstanceOf[Integer].intValue()
    val cols = raw(1).asInstanceOf[Array[AnyRef]]
    decodeRows(cols, outRows, output)
  }

  @transient private lazy val rdd: RDD[InternalRow] = {
    if (resultRows.isEmpty) {
      sparkContext.emptyRDD
    } else {
      val n = math.min(resultRows.length, session.leafNodeDefaultParallelism)
      sparkContext.parallelize(resultRows.toImmutableArraySeq, n)
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    rdd.map { r =>
      numOutputRows += 1
      r
    }
  }

  override def executeCollect(): Array[InternalRow] = {
    longMetric("numOutputRows") += resultRows.length
    resultRows
  }

  override def outputPartitioning: Partitioning = super.outputPartitioning

  override protected def stringArgs: Iterator[Any] =
    Iterator(compiled.ir, output)

  private def encodeLeaves(
      leaves: Seq[NativeSqlPlan.LeafData]): (Array[Array[AnyRef]], Array[Int]) = {
    val batches = new Array[Array[AnyRef]](math.max(leaves.length, 1))
    val counts = new Array[Int](math.max(leaves.length, 1))
    leaves.zipWithIndex.foreach { case (leaf, i) =>
      leaf match {
        case NativeSqlPlan.LocalLeaf(schema, rows) =>
          batches(i) = encodeLocal(schema, rows)
          counts(i) = rows.length
        case NativeSqlPlan.RangeLeaf(start, end, step) =>
          // Range is generated in C++; pass empty batch + encode params in IR.
          batches(i) = Array.empty[AnyRef]
          counts(i) = 0
          val _ = (start, end, step)
      }
    }
    if (leaves.isEmpty) {
      batches(0) = Array.empty[AnyRef]
      counts(0) = 0
    }
    (batches, counts)
  }

  private def encodeLocal(schema: Seq[Attribute], rows: Seq[InternalRow]): Array[AnyRef] = {
    val n = rows.length
    schema.iterator.zipWithIndex.map { case (a, col) =>
      a.dataType match {
        case IntegerType | ByteType | ShortType =>
          val arr = new Array[Int](n)
          var i = 0
          while (i < n) {
            if (!rows(i).isNullAt(col)) {
              a.dataType match {
                case IntegerType => arr(i) = rows(i).getInt(col)
                case ByteType => arr(i) = rows(i).getByte(col)
                case ShortType => arr(i) = rows(i).getShort(col)
                case _ =>
              }
            }
            i += 1
          }
          arr
        case LongType =>
          val arr = new Array[Long](n)
          var i = 0
          while (i < n) {
            if (!rows(i).isNullAt(col)) arr(i) = rows(i).getLong(col)
            i += 1
          }
          arr
        case DoubleType | FloatType =>
          val arr = new Array[Double](n)
          var i = 0
          while (i < n) {
            if (!rows(i).isNullAt(col)) {
              if (a.dataType == DoubleType) arr(i) = rows(i).getDouble(col)
              else arr(i) = rows(i).getFloat(col).toDouble
            }
            i += 1
          }
          arr
        case BooleanType =>
          val arr = new Array[Boolean](n)
          var i = 0
          while (i < n) {
            if (!rows(i).isNullAt(col)) arr(i) = rows(i).getBoolean(col)
            i += 1
          }
          arr
        case other =>
          throw new UnsupportedOperationException(s"Native SQL cannot encode $other")
      }
    }.toArray
  }

  private def decodeRows(
      cols: Array[AnyRef],
      n: Int,
      schema: Seq[Attribute]): Array[InternalRow] = {
    val proj = UnsafeProjection.create(schema, schema)
    val out = new Array[InternalRow](n)
    var i = 0
    while (i < n) {
      val generic = new GenericInternalRow(schema.length)
      var c = 0
      while (c < schema.length) {
        (schema(c).dataType, cols(c)) match {
          case (IntegerType | ByteType | ShortType, a: Array[Int]) => generic.setInt(c, a(i))
          case (IntegerType | ByteType | ShortType, a: Array[Long]) => generic.setInt(c, a(i).toInt)
          case (LongType, a: Array[Long]) => generic.setLong(c, a(i))
          case (LongType, a: Array[Int]) => generic.setLong(c, a(i).toLong)
          case (DoubleType | FloatType, a: Array[Double]) => generic.setDouble(c, a(i))
          case (BooleanType, a: Array[Boolean]) => generic.setBoolean(c, a(i))
          case _ =>
        }
        c += 1
      }
      out(i) = proj(generic).copy()
      i += 1
    }
    out
  }
}
