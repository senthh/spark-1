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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, Path}

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Spark plans splits; C++ parquet-cpp reads them. Same shape as Velox / ClickHouse.
 */
object NativeParquet {

  val NS_I32 = 1
  val NS_I64 = 2
  val NS_F64 = 3
  val NS_BOOL = 4

  final class PreparedScan(val spec: Array[AnyRef], val temps: Array[File]) {
    def cleanup(): Unit = {
      var i = 0
      while (i < temps.length) {
        temps(i).delete()
        i += 1
      }
    }
  }

  def nsType(dt: DataType): Int = dt match {
    case IntegerType | ByteType | ShortType | DateType => NS_I32
    case LongType | TimestampType => NS_I64
    case DoubleType | FloatType => NS_F64
    case BooleanType => NS_BOOL
    case _: DecimalType => NS_I64
    case StringType => NS_I64
    case _ => NS_I64
  }

  def filePartitions(scan: FileSourceScanExec): Array[FilePartition] = {
    try {
      scan.inputRDD.partitions.collect { case f: FilePartition => f }
    } catch {
      case _: Exception => Array.empty[FilePartition]
    }
  }

  def allFiles(scan: FileSourceScanExec): Array[PartitionedFile] = {
    val fromRdd = filePartitions(scan).flatMap(_.files)
    if (fromRdd.nonEmpty) {
      fromRdd
    } else {
      try {
        val listed = scan.selectedPartitions.toPartitionArray
        if (listed != null) listed else Array.empty[PartitionedFile]
      } catch {
        case _: Exception => Array.empty[PartitionedFile]
      }
    }
  }

  /**
   * JNI scan spec: paths, starts, lengths, blobs, names, types, preds.
   * HDFS files <= 16MB are opened in C++ via Hadoop FS (no /tmp copy).
   * Larger fact files are copied locally until random HDFS reads are stable.
   */
  def scanSpec(
      files: Array[PartitionedFile],
      schema: Seq[Attribute],
      hadoopConf: Configuration,
      reuseRemote: Boolean = false,
      usedCols: Set[Int] = Set.empty,
      filters: Seq[Expression] = Nil): PreparedScan = {
    val n = files.length
    val paths = new Array[String](n)
    val starts = new Array[Long](n)
    val lengths = new Array[Long](n)
    val blobs = new Array[AnyRef](n)
    val temps = new java.util.ArrayList[File]()
    var i = 0
    while (i < n) {
      val f = files(i)
      val hp = f.toPath
      val local = localFile(hp)
      if (local != null) {
        paths(i) = local
      } else if (remoteLen(hp, hadoopConf) <= 16L * 1024 * 1024) {
        paths(i) = hp.toString
      } else {
        val tmp = copyToLocal(hp, hadoopConf)
        paths(i) = tmp.getAbsolutePath
        temps.add(tmp)
      }
      blobs(i) = null
      starts(i) = f.start
      lengths(i) = f.length
      i += 1
    }
    /* Empty name => C++ zeros the column (no parquet read). IR cN is positional. */
    val names = schema.iterator.zipWithIndex.map { case (a, i) =>
      if (usedCols.isEmpty || usedCols.contains(i)) a.name else ""
    }.toArray
    val types = schema.map(a => nsType(a.dataType)).toArray
    val (pcols, pops, pvals) = encodeFilters(filters, schema)
    new PreparedScan(
      Array(paths, starts, lengths, blobs, names, types,
        Array(pcols, pops, pvals).asInstanceOf[Array[AnyRef]]),
      temps.toArray(new Array[File](0)))
  }

  /** Integer comparisons Spark already pushed as dataFilters (plus AND / BETWEEN). */
  def encodeFilters(
      filters: Seq[Expression],
      schema: Seq[Attribute]): (Array[Int], Array[Int], Array[Long]) = {
    val cols = new scala.collection.mutable.ArrayBuffer[Int]()
    val ops = new scala.collection.mutable.ArrayBuffer[Int]()
    val vals = new scala.collection.mutable.ArrayBuffer[Long]()
    def litLong(e: Expression): Option[Long] = e match {
      case Literal(v, IntegerType) => Some(v.asInstanceOf[Int].toLong)
      case Literal(v, LongType) => Some(v.asInstanceOf[Long])
      case Literal(v, DateType) => Some(v.asInstanceOf[Int].toLong)
      case Literal(v, ShortType) => Some(v.asInstanceOf[Short].toLong)
      case Literal(v, ByteType) => Some(v.asInstanceOf[Byte].toLong)
      case Literal(v, BooleanType) => Some(if (v.asInstanceOf[Boolean]) 1L else 0L)
      case _ => None
    }
    def colIdx(e: Expression): Option[Int] = e match {
      case a: Attribute =>
        val byId = schema.indexWhere(_.exprId == a.exprId)
        if (byId >= 0) {
          Some(byId)
        } else {
          val byName = schema.indexWhere(_.name == a.name)
          if (byName >= 0) Some(byName) else None
        }
      case BoundReference(ordinal, _, _) => Some(ordinal)
      case _ => None
    }
    def add(op: Int, col: Expression, lit: Expression): Unit = {
      (colIdx(col), litLong(lit)) match {
        case (Some(c), Some(v)) =>
          cols += c
          ops += op
          vals += v
        case _ =>
      }
    }
    def rec(e: Expression): Unit = e match {
      case And(l, r) =>
        rec(l)
        rec(r)
      case EqualTo(l, r) =>
        if (litLong(r).isDefined) add(1, l, r) else add(1, r, l)
      case GreaterThanOrEqual(l, r) =>
        if (litLong(r).isDefined) add(2, l, r) else add(3, r, l)
      case LessThanOrEqual(l, r) =>
        if (litLong(r).isDefined) add(3, l, r) else add(2, r, l)
      case GreaterThan(l, r) =>
        if (litLong(r).isDefined) add(4, l, r) else add(5, r, l)
      case LessThan(l, r) =>
        if (litLong(r).isDefined) add(5, l, r) else add(4, r, l)
      case b: Between =>
        rec(GreaterThanOrEqual(b.input, b.lower))
        rec(LessThanOrEqual(b.input, b.upper))
      case _ =>
    }
    filters.foreach(rec)
    (cols.toArray, ops.toArray, vals.toArray)
  }

  def dictFromResult(raw: Array[AnyRef]): java.util.HashMap[java.lang.Long, UTF8String] = {
    val map = new java.util.HashMap[java.lang.Long, UTF8String]()
    if (raw.length >= 4 && raw(2) != null && raw(3) != null) {
      val hashes = raw(2).asInstanceOf[Array[Long]]
      val vals = raw(3).asInstanceOf[Array[AnyRef]]
      var i = 0
      while (i < hashes.length && i < vals.length) {
        vals(i) match {
          case s: String =>
            map.put(java.lang.Long.valueOf(hashes(i)), UTF8String.fromString(s))
          case s: UTF8String =>
            map.put(java.lang.Long.valueOf(hashes(i)), s)
          case b: Array[Byte] =>
            map.put(java.lang.Long.valueOf(hashes(i)), UTF8String.fromBytes(b))
          case _ =>
        }
        i += 1
      }
    }
    map
  }

  private def localFile(p: Path): String = {
    if (p == null) {
      null
    } else {
      val scheme = p.toUri.getScheme
      if (scheme == null || scheme == "file") {
        val f = new File(p.toUri.getPath)
        if (f.isFile) f.getAbsolutePath else null
      } else {
        null
      }
    }
  }

  private def remoteLen(p: Path, conf: Configuration): Long = {
    try {
      p.getFileSystem(conf).getFileStatus(p).getLen
    } catch {
      case _: Exception => Long.MaxValue
    }
  }

  private def copyToLocal(p: Path, conf: Configuration): File = {
    val fs = p.getFileSystem(conf)
    val tmp = File.createTempFile("nativesql-", ".parquet")
    tmp.deleteOnExit()
    if (!FileUtil.copy(fs, p, tmp, false, conf)) {
      tmp.delete()
      throw new IllegalStateException(s"cannot copy parquet to local: $p")
    }
    tmp
  }
}
