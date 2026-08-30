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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Extract primitive columns from a Spark vectorized batch into JNI arrays.
 * Walks [[ColumnVector]] once per column -- no InternalRow.copy of the partition.
 */
object NativeColumnar {

  /** One join-key column as a JNI batch (late materialization). */
  def encodeKeyCol(batch: ColumnarBatch, schema: Seq[Attribute], key: Int): Array[AnyRef] = {
    val k = clampKey(key, math.min(batch.numCols(), schema.length))
    val out = new Array[AnyRef](1)
    out(0) = encodeCol(
      batch.column(k), schema(k).dataType, batch.numRows(),
      new java.util.HashMap[java.lang.Long, UTF8String]())
    out
  }

  def encodeKeyCol(
      rows: Array[InternalRow],
      schema: Seq[Attribute],
      key: Int): Array[AnyRef] = {
    val n = rows.length
    val k = clampKey(key, schema.length)
    val dt = schema(k).dataType
    val col = dt match {
      case IntegerType | ByteType | ShortType | DateType =>
        val arr = new Array[Int](n)
        var i = 0
        while (i < n) {
          if (!rows(i).isNullAt(k)) arr(i) = rows(i).getInt(k)
          i += 1
        }
        arr
      case LongType | TimestampType =>
        val arr = new Array[Long](n)
        var i = 0
        while (i < n) {
          if (!rows(i).isNullAt(k)) arr(i) = rows(i).getLong(k)
          i += 1
        }
        arr
      case _ =>
        val arr = new Array[Long](n)
        var i = 0
        while (i < n) {
          if (!rows(i).isNullAt(k)) arr(i) = rows(i).getLong(k)
          i += 1
        }
        arr
    }
    val out = new Array[AnyRef](1)
    out(0) = col.asInstanceOf[AnyRef]
    out
  }

  private def clampKey(key: Int, n: Int): Int = {
    if (n <= 0) 0
    else if (key < 0) 0
    else if (key >= n) 0
    else key
  }

  def encodeBatch(batch: ColumnarBatch, schema: Seq[Attribute]): Array[AnyRef] = {
    encodeBatchMapped(batch, schema)._1
  }

  /**
   * Encode a batch and a hash-to-string map so C++ can group by hashed
   * strings and Spark can recover the original UTF8 values.
   */
  def encodeBatchMapped(
      batch: ColumnarBatch,
      schema: Seq[Attribute]): (Array[AnyRef], java.util.HashMap[java.lang.Long, UTF8String]) = {
    val n = batch.numRows()
    val map = new java.util.HashMap[java.lang.Long, UTF8String]()
    val cols = schema.iterator.zipWithIndex.map { case (a, i) =>
      encodeCol(batch.column(i), a.dataType, n, map)
    }.toArray
    (cols, map)
  }

  def encodeRowsMapped(
      rows: Array[InternalRow],
      schema: Seq[Attribute]): (Array[AnyRef], java.util.HashMap[java.lang.Long, UTF8String]) = {
    val n = rows.length
    val map = new java.util.HashMap[java.lang.Long, UTF8String]()
    val cols = schema.iterator.zipWithIndex.map { case (a, i) =>
      encodeRowCol(rows, i, a.dataType, n, map)
    }.toArray
    (cols, map)
  }

  private def encodeRowCol(
      rows: Array[InternalRow],
      col: Int,
      dt: DataType,
      n: Int,
      map: java.util.HashMap[java.lang.Long, UTF8String]): AnyRef = dt match {
    case StringType =>
      val arr = new Array[Long](n)
      var i = 0
      while (i < n) {
        if (!rows(i).isNullAt(col)) {
          val s = rows(i).getUTF8String(col)
          val h = NativeSqlPlan.hash64(s)
          arr(i) = h
          map.put(java.lang.Long.valueOf(h), s)
        }
        i += 1
      }
      arr
    case d: DecimalType if d.precision <= 18 =>
      val arr = new Array[Long](n)
      var i = 0
      while (i < n) {
        if (!rows(i).isNullAt(col)) {
          arr(i) = rows(i).getDecimal(col, d.precision, d.scale).toUnscaledLong
        }
        i += 1
      }
      arr
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

  private def encodeCol(
      cv: ColumnVector,
      dt: DataType,
      n: Int,
      map: java.util.HashMap[java.lang.Long, UTF8String]): AnyRef = dt match {
    case IntegerType | ByteType | ShortType | DateType =>
      val arr = new Array[Int](n)
      var i = 0
      while (i < n) {
        if (!cv.isNullAt(i)) {
          dt match {
            case ByteType => arr(i) = cv.getByte(i)
            case ShortType => arr(i) = cv.getShort(i)
            case _ => arr(i) = cv.getInt(i)
          }
        }
        i += 1
      }
      arr
    case LongType | TimestampType =>
      val arr = new Array[Long](n)
      var i = 0
      while (i < n) {
        if (!cv.isNullAt(i)) arr(i) = cv.getLong(i)
        i += 1
      }
      arr
    case DoubleType | FloatType =>
      val arr = new Array[Double](n)
      var i = 0
      while (i < n) {
        if (!cv.isNullAt(i)) {
          if (dt == DoubleType) arr(i) = cv.getDouble(i)
          else arr(i) = cv.getFloat(i).toDouble
        }
        i += 1
      }
      arr
    case BooleanType =>
      val arr = new Array[Boolean](n)
      var i = 0
      while (i < n) {
        if (!cv.isNullAt(i)) arr(i) = cv.getBoolean(i)
        i += 1
      }
      arr
    case StringType =>
      val arr = new Array[Long](n)
      var i = 0
      while (i < n) {
        if (!cv.isNullAt(i)) {
          val s = cv.getUTF8String(i)
          val h = NativeSqlPlan.hash64(s)
          arr(i) = h
          map.put(java.lang.Long.valueOf(h), s)
        }
        i += 1
      }
      arr
    case d: DecimalType if d.precision <= 18 =>
      val arr = new Array[Long](n)
      var i = 0
      while (i < n) {
        if (!cv.isNullAt(i)) {
          arr(i) = cv.getDecimal(i, d.precision, d.scale).toUnscaledLong
        }
        i += 1
      }
      arr
    case _ =>
      new Array[Long](n)
  }

  /**
   * Stitch left/right payload rows from hashjoinidx matches. Preserves
   * strings and decimals that C++ never encoded.
   */
  def gatherJoin(
      leftIdx: Array[Int],
      rightIdx: Array[Int],
      n: Int,
      leftAt: Int => InternalRow,
      rightAt: Int => InternalRow,
      leftWidth: Int,
      rightWidth: Int,
      outSchema: Seq[Attribute]): Array[InternalRow] = {
    val proj = UnsafeProjection.create(outSchema, outSchema)
    val out = new Array[InternalRow](n)
    val generic = new GenericInternalRow(outSchema.length)
    var i = 0
    var o = 0
    while (i < n && i < leftIdx.length && i < rightIdx.length) {
      val l = leftAt(leftIdx(i))
      val r = rightAt(rightIdx(i))
      val lf = l.numFields
      val rf = r.numFields
      var c = 0
      while (c < leftWidth && c < lf && c < outSchema.length) {
        if (l.isNullAt(c)) generic.setNullAt(c)
        else generic.update(c, l.get(c, outSchema(c).dataType))
        c += 1
      }
      var d = 0
      while (d < rightWidth && d < rf) {
        val dest = leftWidth + d
        if (dest >= outSchema.length) {
          d = rightWidth
        } else if (r.isNullAt(d)) {
          generic.setNullAt(dest)
          d += 1
        } else {
          generic.update(dest, r.get(d, outSchema(dest).dataType))
          d += 1
        }
      }
      out(o) = proj(generic).copy()
      o += 1
      i += 1
    }
    if (o == n) out else java.util.Arrays.copyOf(out, o)
  }
}
