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

import java.math.{BigDecimal => JBigDecimal, BigInteger}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.hadoop.fs.Path
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.ParquetFileReader
import org.apache.parquet.hadoop.ParquetReader
import org.apache.parquet.hadoop.example.GroupReadSupport
import org.apache.parquet.hadoop.util.HadoopInputFile
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName

import org.apache.spark.sql.vegam.VegamPaths
import org.apache.spark.sql.vegam.plan.{FileRef, FilterPred, NativePlan}

/**
 * Parquet I/O through the Java Hadoop client. Short-circuit and libhdfs are
 * off. Remote (hdfs/s3) and POSIX paths use the same reader.
 */
object ParquetIO {

  def loadNative(
      path: String,
      columns: Array[String],
      partKeys: Array[String],
      partVals: Array[String]): NativeTable = {
    val parts = if (partKeys == null || partVals == null) {
      Seq.empty
    } else {
      partKeys.toSeq.zip(partVals.toSeq)
    }
    val table = read(FileRef(path, parts), Option(columns).map(_.toSeq).getOrElse(Nil), Nil)
    val t = new NativeTable()
    t.numRows = table.rows.length
    t.numCols = table.names.length
    t.names = table.names
    t.values = new Array[Double](t.numRows * t.numCols)
    t.nulls = new Array[Boolean](t.numRows * t.numCols)
    t.texts = new Array[String](t.numRows * t.numCols)
    var r = 0
    while (r < t.numRows) {
      val row = table.rows(r)
      var c = 0
      while (c < t.numCols) {
        val i = r * t.numCols + c
        val cell = row(c)
        if (cell == null) {
          t.nulls(i) = true
          t.values(i) = Double.NaN
        } else {
          cell match {
            case s: String =>
              t.texts(i) = s
              t.values(i) = toDouble(s).getOrElse(Double.NaN)
            case n: java.lang.Number =>
              t.values(i) = n.doubleValue()
            case other =>
              t.texts(i) = other.toString
              t.values(i) = toDouble(other).getOrElse(Double.NaN)
          }
        }
        c += 1
      }
      r += 1
    }
    t
  }


  final class Table(
      val names: Array[String],
      val rows: ArrayBuffer[Array[Any]]) {
    def colIndex(name: String): Int = names.indexOf(name)
  }

  def footerRows(path: String): Long = {
    val reader = ParquetFileReader.open(
      HadoopInputFile.fromPath(new Path(path), VegamPaths.hadoopConf()))
    try {
      reader.getFooter.getBlocks.asScala.map(_.getRowCount).sum
    } finally {
      reader.close()
    }
  }

  def read(ref: FileRef, want: Seq[String], filters: Seq[FilterPred]): Table = {
    val names = (want ++ ref.parts.map(_._1) ++ filters.map(_.col)).distinct.toArray
    val rows = new ArrayBuffer[Array[Any]]()
    val reader = ParquetReader.builder(new GroupReadSupport(), new Path(ref.path))
      .withConf(VegamPaths.hadoopConf())
      .build()
    try {
      var g = reader.read()
      while (g != null) {
        val row = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val n = names(i)
          val part = ref.parts.find(_._1 == n)
          if (part.isDefined) {
            row(i) = coercePart(part.get._2)
          } else {
            row(i) = readAny(g, n).orNull
          }
          i += 1
        }
        if (keep(row, names, filters)) {
          rows += row
        }
        g = reader.read()
      }
    } finally {
      reader.close()
    }
    new Table(names, rows)
  }

  def keep(row: Array[Any], names: Array[String], filters: Seq[FilterPred]): Boolean = {
    filters.forall { f =>
      val i = names.indexOf(f.col)
      if (i < 0) {
        false
      } else {
        cmp(row(i), f)
      }
    }
  }

  def cmp(cell: Any, f: FilterPred): Boolean = {
    if (cell == null) {
      false
    } else if (f.isString) {
      val s = cell.toString
      f.op match {
        case NativePlan.FILTER_EQ => s == f.strValue
        case NativePlan.FILTER_NE => s != f.strValue
        case _ => false
      }
    } else {
      val n = toDouble(cell)
      if (n.isEmpty) {
        false
      } else {
        val v = n.get
        val t = f.numValue
        f.op match {
          case NativePlan.FILTER_GT => v > t
          case NativePlan.FILTER_GTE => v >= t
          case NativePlan.FILTER_LT => v < t
          case NativePlan.FILTER_LTE => v <= t
          case NativePlan.FILTER_EQ => v == t
          case NativePlan.FILTER_NE => v != t
          case _ => false
        }
      }
    }
  }

  def toDouble(cell: Any): Option[Double] = cell match {
    case null => None
    case d: java.lang.Double => Some(d.doubleValue())
    case l: java.lang.Long => Some(l.doubleValue())
    case i: java.lang.Integer => Some(i.doubleValue())
    case f: java.lang.Float => Some(f.doubleValue())
    case s: String =>
      try {
        Some(s.toDouble)
      } catch {
        case _: NumberFormatException => None
      }
    case _ =>
      try {
        Some(cell.toString.toDouble)
      } catch {
        case _: NumberFormatException => None
      }
  }

  def toLong(cell: Any): Option[Long] = cell match {
    case null => None
    case l: java.lang.Long => Some(l.longValue())
    case i: java.lang.Integer => Some(i.longValue())
    case d: java.lang.Double => Some(d.longValue())
    case s: String =>
      try {
        Some(s.toLong)
      } catch {
        case _: NumberFormatException => toDouble(s).map(_.toLong)
      }
    case _ => toDouble(cell).map(_.toLong)
  }

  private def coercePart(v: String): Any = {
    try {
      java.lang.Long.valueOf(v)
    } catch {
      case _: NumberFormatException =>
        try {
          java.lang.Double.valueOf(v)
        } catch {
          case _: NumberFormatException => v
        }
    }
  }

  private def readAny(g: Group, name: String): Option[Any] = {
    field(g, name).map {
      case (idx, ptn) =>
        val scale = parquetScale(g, idx)
        ptn match {
          case PrimitiveTypeName.INT32 if scale > 0 =>
            JBigDecimal.valueOf(g.getInteger(idx, 0).toLong, scale).doubleValue()
          case PrimitiveTypeName.INT64 if scale > 0 =>
            JBigDecimal.valueOf(g.getLong(idx, 0), scale).doubleValue()
          case PrimitiveTypeName.INT32 => g.getInteger(idx, 0).toLong
          case PrimitiveTypeName.INT64 => g.getLong(idx, 0)
          case PrimitiveTypeName.DOUBLE => g.getDouble(idx, 0)
          case PrimitiveTypeName.FLOAT => g.getFloat(idx, 0).toDouble
          case PrimitiveTypeName.BOOLEAN => if (g.getBoolean(idx, 0)) 1L else 0L
          case PrimitiveTypeName.BINARY | PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY if scale > 0 =>
            new JBigDecimal(new BigInteger(g.getBinary(idx, 0).getBytes), scale).doubleValue()
          case PrimitiveTypeName.BINARY | PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY =>
            g.getBinary(idx, 0).toStringUsingUTF8
          case _ => null
        }
    }
  }

  private def parquetScale(g: Group, idx: Int): Int = {
    g.getType.getType(idx).asPrimitiveType().getLogicalTypeAnnotation match {
      case d: DecimalLogicalTypeAnnotation => d.getScale
      case _ => 0
    }
  }

  private def field(g: Group, name: String): Option[(Int, PrimitiveTypeName)] = {
    try {
      val t = g.getType
      if (!t.containsField(name) || g.getFieldRepetitionCount(name) == 0) {
        None
      } else {
        val idx = t.getFieldIndex(name)
        Some((idx, t.getType(idx).asPrimitiveType().getPrimitiveTypeName))
      }
    } catch {
      case NonFatal(_) => None
    }
  }
}
