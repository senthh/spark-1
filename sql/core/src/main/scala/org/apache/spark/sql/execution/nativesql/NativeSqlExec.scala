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
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.types._
import org.apache.spark.util.ArrayImplicits._

/**
 * Native SQL subtree.
 *
 * In-memory leaves (LocalRelation / Range): run once on the driver via JNI.
 * File-backed leaves: [[children]] is a FileSourceScanExec; each partition is
 * encoded, filtered/projected in C++, then gathered back to InternalRows.
 * String/decimal columns are dummy-encoded; a row-id column is appended so
 * C++ returns kept indices and this node copies the original rows.
 */
case class NativeSqlExec(
    compiled: NativeSqlPlan.Compiled,
    override val children: Seq[SparkPlan] = Nil) extends SparkPlan {

  override def output: Seq[Attribute] = compiled.output

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def outputPartitioning: Partitioning = {
    if (children.size == 1) children.head.outputPartitioning
    else UnknownPartitioning(0)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    if (children.nonEmpty) {
      executeFileBacked()
    } else {
      executeInMemory()
    }
  }

  override def executeCollect(): Array[InternalRow] = {
    if (children.nonEmpty) {
      super.executeCollect()
    } else {
      val rows = inMemoryRows
      longMetric("numOutputRows") += rows.length
      rows
    }
  }

  override protected def stringArgs: Iterator[Any] =
    Iterator(compiled.ir, output, children.size)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]): SparkPlan =
    copy(children = newChildren)

  private def executeInMemory(): RDD[InternalRow] = {
    val rows = inMemoryRows
    val numOutputRows = longMetric("numOutputRows")
    if (rows.isEmpty) {
      sparkContext.emptyRDD
    } else {
      val n = math.min(rows.length, session.leafNodeDefaultParallelism)
      sparkContext.parallelize(rows.toImmutableArraySeq, n).map { r =>
        numOutputRows += 1
        r
      }
    }
  }

  @transient private lazy val inMemoryRows: Array[InternalRow] = {
    val (batches, numRows) = encodeLeaves(compiled.leaves)
    val raw = NativeSqlLibrary.execute(compiled.ir, batches, numRows)
    decodeResult(raw, output)
  }

  /**
   * Per-partition path: append a row-id column, run
   * `(project (list cN) COMPILED_IR)`, gather original rows.
   * The partition function lives on the companion so the SparkPlan
   * (and its FileIndex) is not serialized to executors.
   */
  private def executeFileBacked(): RDD[InternalRow] = {
    val rdd = children.head.execute()
    val schema = children.head.output.toArray.toSeq
    val ir = compiled.ir
    val metric = longMetric("numOutputRows")
    NativeSqlExec.evalFilePartition(rdd, schema, ir, metric)
  }

  private def encodeLeaves(
      leaves: Seq[NativeSqlPlan.LeafData]): (Array[Array[AnyRef]], Array[Int]) = {
    val batches = new Array[Array[AnyRef]](math.max(leaves.length, 1))
    val counts = new Array[Int](math.max(leaves.length, 1))
    leaves.zipWithIndex.foreach { case (leaf, i) =>
      leaf match {
        case NativeSqlPlan.LocalLeaf(schema, rows) =>
          batches(i) = NativeSqlExec.encodePartition(schema, rows.toArray, withRowId = false)
          counts(i) = rows.length
        case NativeSqlPlan.RangeLeaf(start, end, step) =>
          batches(i) = Array.empty[AnyRef]
          counts(i) = 0
          val _ = (start, end, step)
        case NativeSqlPlan.FileLeaf(_) =>
          batches(i) = Array.empty[AnyRef]
          counts(i) = 0
      }
    }
    if (leaves.isEmpty) {
      batches(0) = Array.empty[AnyRef]
      counts(0) = 0
    }
    (batches, counts)
  }

  private def decodeResult(raw: Array[AnyRef], schema: Seq[Attribute]): Array[InternalRow] = {
    val outRows = raw(0).asInstanceOf[Integer].intValue()
    val cols = raw(1).asInstanceOf[Array[AnyRef]]
    NativeSqlExec.decodeRows(cols, outRows, schema)
  }
}

object NativeSqlExec {

  def evalFilePartition(
      rdd: RDD[InternalRow],
      schema: Seq[Attribute],
      ir: String,
      numOutputRows: SQLMetric): RDD[InternalRow] = {
    val wrappedIr = s"(project (list c${schema.length}) $ir)"
    rdd.mapPartitionsInternal { iter =>
      val rows = iter.map(_.copy()).toArray
      if (rows.isEmpty) {
        Iterator.empty
      } else {
        val cols = encodePartition(schema, rows, withRowId = true)
        val raw = NativeSqlLibrary.execute(wrappedIr, Array(cols), Array(rows.length))
        val ids = rowIds(raw)
        val out = new Array[InternalRow](ids.length)
        var i = 0
        while (i < ids.length) {
          val idx = ids(i)
          if (idx >= 0 && idx < rows.length) {
            out(i) = rows(idx)
          }
          i += 1
        }
        numOutputRows += out.length
        out.iterator
      }
    }
  }

  def encodePartition(
      schema: Seq[Attribute],
      rows: Array[InternalRow],
      withRowId: Boolean): Array[AnyRef] = {
    val n = rows.length
    val base = schema.iterator.zipWithIndex.map { case (a, col) =>
      encodeCol(a.dataType, rows, col, n)
    }.toArray
    if (!withRowId) {
      base
    } else {
      val ids = new Array[Int](n)
      var i = 0
      while (i < n) {
        ids(i) = i
        i += 1
      }
      base :+ ids
    }
  }

  private def encodeCol(dt: DataType, rows: Array[InternalRow], col: Int, n: Int): AnyRef = {
    dt match {
      case IntegerType | ByteType | ShortType | DateType =>
        val arr = new Array[Int](n)
        var i = 0
        while (i < n) {
          if (!rows(i).isNullAt(col)) {
            dt match {
              case IntegerType | DateType => arr(i) = rows(i).getInt(col)
              case ByteType => arr(i) = rows(i).getByte(col)
              case ShortType => arr(i) = rows(i).getShort(col)
              case _ =>
            }
          }
          i += 1
        }
        arr
      case LongType | TimestampType =>
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
            if (dt == DoubleType) arr(i) = rows(i).getDouble(col)
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
      case _ =>
        new Array[Long](n)
    }
  }

  def rowIds(raw: Array[AnyRef]): Array[Int] = {
    val n = raw(0).asInstanceOf[Integer].intValue()
    val cols = raw(1).asInstanceOf[Array[AnyRef]]
    if (n == 0 || cols.isEmpty) {
      Array.empty
    } else {
      cols(0) match {
        case a: Array[Int] =>
          val out = new Array[Int](n)
          System.arraycopy(a, 0, out, 0, n)
          out
        case a: Array[Long] =>
          val out = new Array[Int](n)
          var i = 0
          while (i < n) {
            out(i) = a(i).toInt
            i += 1
          }
          out
        case _ => Array.empty
      }
    }
  }

  def decodeRows(
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
          case (IntegerType | ByteType | ShortType | DateType, a: Array[Int]) =>
            generic.setInt(c, a(i))
          case (IntegerType | ByteType | ShortType | DateType, a: Array[Long]) =>
            generic.setInt(c, a(i).toInt)
          case (LongType | TimestampType, a: Array[Long]) => generic.setLong(c, a(i))
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
