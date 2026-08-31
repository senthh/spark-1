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

import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.nativesql.NativeSqlPlan.NativeAggKind
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ArrayImplicits._

/**
 * Native SQL subtree.
 *
 * In-memory leaves (LocalRelation / Range): run once on the driver via JNI.
 * File-backed trees: Spark plans splits; every leaf is a C++ parquet scan
 * (probe partition + broadcast dim file lists). No driver-side JNI pack of
 * dims. When native scan is off, dims stay Java arrays and the probe is
 * ColumnarBatch / InternalRow.
 */
case class NativeSqlExec(
    compiled: NativeSqlPlan.Compiled,
    override val children: Seq[SparkPlan] = Nil) extends SparkPlan {

  override def output: Seq[Attribute] = compiled.output

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def outputPartitioning: Partitioning = {
    if (children.isEmpty) {
      UnknownPartitioning(0)
    } else {
      children.find(c => !isBroadcastDim(c))
        .getOrElse(children.head).outputPartitioning
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    if (children.nonEmpty) {
      executeFilePipeline()
    } else {
      executeInMemory()
    }
  }

  /**
   * Leftover Spark parents (BHJ build via IdentityBroadcast, CollectLimit)
   * call executeBroadcast. Collect the native result instead of throwing
   * the empty UnsupportedOperationException that aborted Q3/Q6/Q8/Q10/Q11.
   */
  override protected[sql] def doExecuteBroadcast[T](): Broadcast[T] = {
    sparkContext.broadcast(executeCollect().asInstanceOf[T])(
      ClassTag.AnyRef.asInstanceOf[ClassTag[T]])
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
   * File pipeline: C++ parquet-scans every leaf (probe split + dim files),
   * runs the full IR, decodes, then merges partial hashagg.
   */
  private def isBroadcastDim(p: SparkPlan): Boolean = broadcastOf(p).isDefined

  private def broadcastOf(p: SparkPlan): Option[BroadcastExchangeExec] = p match {
    case b: BroadcastExchangeExec => Some(b)
    case ReusedExchangeExec(_, b: BroadcastExchangeExec) => Some(b)
    case _ => None
  }

  private def fileScanOf(p: SparkPlan): Option[FileSourceScanExec] = p match {
    case f: FileSourceScanExec => Some(f)
    case other =>
      broadcastOf(other) match {
        case Some(b) => b.collectFirst { case f: FileSourceScanExec => f }
        case None => other.collectFirst { case f: FileSourceScanExec => f }
      }
  }

  /* Column indexes referenced on the path from (scan N) up through project/filter. */
  private def usedColsForScan(ir: String, scanIdx: Int): Set[Int] = {
    val needle = s"(scan $scanIdx)"
    val cols = scala.collection.mutable.Set[Int]()
    val colRe = """c(\d+)""".r
    var from = 0
    while (from < ir.length) {
      val p = ir.indexOf(needle, from)
      if (p < 0) {
        return cols.toSet
      }
      var depth = 0
      var s = p
      var seen = 0
      while (s > 0 && seen < 8) {
        s -= 1
        if (ir.charAt(s) == ')') {
          depth += 1
        } else if (ir.charAt(s) == '(') {
          if (depth == 0) {
            val slice = ir.substring(s, p)
            if (slice.startsWith("(project") || slice.startsWith("(filter") ||
                slice.startsWith("(scan") || slice.startsWith("(hashagg") ||
                slice.startsWith("(segagg") ||
                slice.startsWith("(hashjoin") || slice.startsWith("(hashsemi") ||
                slice.startsWith("(sort") || slice.startsWith("(union")) {
              colRe.findAllMatchIn(slice).foreach { m =>
                cols += m.group(1).toInt
              }
              seen += 1
            } else {
              seen = 99
            }
          } else {
            depth -= 1
          }
        }
      }
      from = p + needle.length
    }
    cols.toSet
  }

  private def executeFilePipeline(): RDD[InternalRow] = {
    val n = children.size
    val probeIdx = children.indexWhere(c => !isBroadcastDim(c))
    val probe = if (probeIdx >= 0) children(probeIdx) else children.head
    val ir = compiled.ir
    val kinds = compiled.aggKinds
    val out = compiled.output
    val metric = longMetric("numOutputRows")
    val nativeScan = SQLConf.get.nativeSqlScanEnabled
    val probeScan = fileScanOf(probe)
    val leafOuts = compiled.withLeafOutputs.leafOutputs
    def schemaFor(idx: Int, fallback: Seq[Attribute]): Seq[Attribute] = {
      if (idx >= 0 && idx < leafOuts.length && leafOuts(idx).nonEmpty) leafOuts(idx) else fallback
    }
    val build = new Array[NativeSqlExec.BuildSide](n)
    val dimFiles = new Array[Broadcast[Array[PartitionedFile]]](n)
    val dimOut = new Array[Seq[Attribute]](n)
    val dimFilters = new Array[Seq[org.apache.spark.sql.catalyst.expressions.Expression]](n)
    var i = 0
    while (i < n) {
      broadcastOf(children(i)) match {
        case Some(b) =>
          val scan = fileScanOf(b)
          val files =
            if (nativeScan && scan.isDefined) {
              NativeParquet.allFiles(scan.get)
            } else {
              Array.empty[PartitionedFile]
            }
          val schema = schemaFor(i, if (scan.isDefined) scan.get.output else b.output)
          if (files.nonEmpty) {
            dimFiles(i) = sparkContext.broadcast(files)
            dimOut(i) = schema
            dimFilters(i) = scan.get.dataFilters
            System.err.println(
              s"nativesql: dim $i files=${files.length} cols=${schema.length} " +
                s"preds=${dimFilters(i).length} " +
                s"used=${usedColsForScan(ir, i).toSeq.sorted.mkString(",")}")
          } else {
            val rows = b.executeBroadcast[Array[InternalRow]]().value
            val encSchema = if (rows.nonEmpty && rows.head.numFields == schema.length) {
              schema
            } else {
              b.output
            }
            val (cols, map) = NativeColumnar.encodeRowsMapped(rows, encSchema)
            build(i) = (sparkContext.broadcast(cols), sparkContext.broadcast(map), rows.length)
            System.err.println(
              s"nativesql: dim $i java-build rows=${rows.length} cols=${encSchema.length}")
          }
        case None =>
      }
      i += 1
    }
    val pIdx = if (probeIdx >= 0) probeIdx else 0
    val rdd = if (nativeScan && probeScan.isDefined) {
      val parts = NativeParquet.filePartitions(probeScan.get)
      val schema = schemaFor(pIdx, probe.output)
      System.err.println(
        s"nativesql: probe $pIdx parts=${parts.length} cols=${schema.length} " +
          s"used=${usedColsForScan(ir, pIdx).toSeq.sorted.mkString(",")}")
      new RDD[InternalRow](sparkContext, Nil) {
        override def getPartitions: Array[Partition] = parts.asInstanceOf[Array[Partition]]
        override def compute(split: Partition, ctx: TaskContext): Iterator[InternalRow] = {
          val fp = split.asInstanceOf[FilePartition]
          val conf = SparkHadoopUtil.get.conf
          val prepared = new Array[NativeParquet.PreparedScan](n)
          val specs = new Array[AnyRef](n)
          var j = 0
          while (j < n) {
            if (j != pIdx && dimFiles(j) != null) {
              prepared(j) = NativeParquet.scanSpec(
                dimFiles(j).value, dimOut(j), conf, reuseRemote = true,
                usedCols = usedColsForScan(ir, j),
                filters = Option(dimFilters(j)).getOrElse(Nil))
              specs(j) = prepared(j).spec
            }
            j += 1
          }
          def runProbe(files: Array[PartitionedFile]): Iterator[InternalRow] = {
            val probeFilters =
              if (probeScan.isDefined) probeScan.get.dataFilters else Nil
            prepared(pIdx) = NativeParquet.scanSpec(
              files, schema, conf, usedCols = usedColsForScan(ir, pIdx),
              filters = probeFilters)
            specs(pIdx) = prepared(pIdx).spec
            try {
              NativeSqlExec.runNative(ir, kinds, out, pIdx, n, build, specs, metric)
            } finally {
              if (prepared(pIdx) != null) {
                prepared(pIdx).cleanup()
                prepared(pIdx) = null
              }
            }
          }
          try {
            if (fp.files.length <= 1) {
              runProbe(fp.files)
            } else {
              fp.files.iterator.flatMap(f => runProbe(Array(f)))
            }
          } finally {
            var k = 0
            while (k < n) {
              if (k != pIdx && prepared(k) != null) prepared(k).cleanup()
              k += 1
            }
          }
        }
      }
    } else if (probe.supportsColumnar) {
      probe.executeColumnar().mapPartitionsInternal { batches =>
        NativeSqlExec.runBatches(batches, probe.output, ir, kinds, out, pIdx, n, build, metric)
      }
    } else {
      probe.execute().mapPartitionsInternal { iter =>
        val rows = iter.map(_.copy()).toArray
        NativeSqlExec.runRows(rows, probe.output, ir, kinds, out, pIdx, n, build, metric)
      }
    }
    if (kinds.isEmpty) {
      rdd
    } else {
      NativeSqlExec.mergePartials(rdd, kinds, out)
    }
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

  private[nativesql] type BuildSide =
    (Broadcast[Array[AnyRef]], Broadcast[java.util.HashMap[java.lang.Long, UTF8String]], Int)

  private[nativesql] def runBatches(
      batches: Iterator[ColumnarBatch],
      probeSchema: Seq[Attribute],
      ir: String,
      kinds: Seq[NativeAggKind],
      out: Seq[Attribute],
      probeIdx: Int,
      n: Int,
      build: Array[BuildSide],
      numOutputRows: SQLMetric): Iterator[InternalRow] = {
    batches.flatMap { batch =>
      if (batch.numRows() == 0) {
        Iterator.empty
      } else {
        val (cols, map) = NativeColumnar.encodeBatchMapped(batch, probeSchema)
        executeOne(ir, kinds, out, probeIdx, n, build, cols, batch.numRows(), map, numOutputRows)
      }
    }
  }

  private[nativesql] def runNative(
      ir: String,
      kinds: Seq[NativeAggKind],
      out: Seq[Attribute],
      probeIdx: Int,
      n: Int,
      build: Array[BuildSide],
      scanSpecs: Array[AnyRef],
      numOutputRows: SQLMetric): Iterator[InternalRow] = {
    val inputs = new Array[Array[AnyRef]](n)
    val ns = new Array[Int](n)
    val scans = new Array[AnyRef](n)
    val map = new java.util.HashMap[java.lang.Long, UTF8String]()
    var i = 0
    while (i < n) {
      if (scanSpecs != null && i < scanSpecs.length && scanSpecs(i) != null) {
        scans(i) = scanSpecs(i)
        inputs(i) = Array.empty[AnyRef]
        ns(i) = 0
      } else if (build(i) != null) {
        inputs(i) = build(i)._1.value
        ns(i) = build(i)._3
        map.putAll(build(i)._2.value)
      } else {
        inputs(i) = Array.empty[AnyRef]
        ns(i) = 0
      }
      i += 1
    }
    val raw = NativeSqlLibrary.executeScan(ir, inputs, ns, scans)
    map.putAll(NativeParquet.dictFromResult(raw))
    val outN = raw(0).asInstanceOf[Integer].intValue()
    val outCols = raw(1).asInstanceOf[Array[AnyRef]]
    if (kinds.isEmpty) {
      numOutputRows += outN
      decodeRows(outCols, outN, out, map).iterator
    } else {
      decodeWide(outCols, outN, kinds, map).iterator
    }
  }

  private[nativesql] def runRows(
      rows: Array[InternalRow],
      probeSchema: Seq[Attribute],
      ir: String,
      kinds: Seq[NativeAggKind],
      out: Seq[Attribute],
      probeIdx: Int,
      n: Int,
      build: Array[BuildSide],
      numOutputRows: SQLMetric): Iterator[InternalRow] = {
    if (rows.isEmpty) {
      Iterator.empty
    } else {
      val (cols, map) = NativeColumnar.encodeRowsMapped(rows, probeSchema)
      executeOne(ir, kinds, out, probeIdx, n, build, cols, rows.length, map, numOutputRows)
    }
  }

  private def executeOne(
      ir: String,
      kinds: Seq[NativeAggKind],
      out: Seq[Attribute],
      probeIdx: Int,
      n: Int,
      build: Array[BuildSide],
      probeCols: Array[AnyRef],
      probeN: Int,
      probeMap: java.util.HashMap[java.lang.Long, UTF8String],
      numOutputRows: SQLMetric): Iterator[InternalRow] = {
    val inputs = new Array[Array[AnyRef]](n)
    val ns = new Array[Int](n)
    val map = new java.util.HashMap[java.lang.Long, UTF8String]()
    map.putAll(probeMap)
    var i = 0
    while (i < n) {
      if (i == probeIdx) {
        inputs(i) = probeCols
        ns(i) = probeN
      } else if (build(i) != null) {
        inputs(i) = build(i)._1.value
        ns(i) = build(i)._3
        map.putAll(build(i)._2.value)
      } else {
        inputs(i) = Array.empty[AnyRef]
        ns(i) = 0
      }
      i += 1
    }
    val raw = NativeSqlLibrary.execute(ir, inputs, ns)
    val outN = raw(0).asInstanceOf[Integer].intValue()
    val outCols = raw(1).asInstanceOf[Array[AnyRef]]
    if (kinds.isEmpty) {
      numOutputRows += outN
      decodeRows(outCols, outN, out, map).iterator
    } else {
      decodeWide(outCols, outN, kinds, map).iterator
    }
  }

  private[nativesql] def mergePartials(
      rdd: RDD[InternalRow],
      kinds: Seq[NativeAggKind],
      out: Seq[Attribute]): RDD[InternalRow] = {
    val keyIdx = kinds.zipWithIndex.collect { case (NativeAggKind.Pass, i) => i }
    rdd.map { row =>
      val key = keyIdx.map { i =>
        if (row.isNullAt(i)) null else safeGet(row, i)
      }
      (key.toSeq, row.copy())
    }.reduceByKey((a, b) => mergeWide(a, b, kinds), math.max(rdd.getNumPartitions, 1))
      .map { case (_, wide) =>
        finalizeAgg(wide, kinds, out)
      }
  }

  private def safeGet(row: InternalRow, i: Int): Any = {
    if (i < 0 || i >= row.numFields || row.isNullAt(i)) {
      null
    } else {
      row match {
        case g: GenericInternalRow => g.get(i, LongType)
        case _ =>
          try {
            row.getUTF8String(i)
          } catch {
            case _: Exception =>
              try {
                java.lang.Long.valueOf(row.getLong(i))
              } catch {
                case _: Exception =>
                  try {
                    java.lang.Double.valueOf(row.getDouble(i))
                  } catch {
                    case _: Exception => null
                  }
              }
          }
      }
    }
  }

  private def mergeWide(a: InternalRow, b: InternalRow, kinds: Seq[NativeAggKind]): InternalRow = {
    val n = kinds.length
    val g = new GenericInternalRow(n)
    var i = 0
    while (i < n && i < a.numFields && i < b.numFields) {
      kinds(i) match {
        case NativeAggKind.Pass =>
          if (!a.isNullAt(i)) g.update(i, safeGet(a, i))
          else if (!b.isNullAt(i)) g.update(i, safeGet(b, i))
        case NativeAggKind.Min =>
          g.setLong(i, math.min(getLong(a, i), getLong(b, i)))
        case NativeAggKind.Max =>
          g.setLong(i, math.max(getLong(a, i), getLong(b, i)))
        case NativeAggKind.AvgSum =>
          g.setDouble(i, getDouble(a, i) + getDouble(b, i))
        case _ =>
          g.setLong(i, getLong(a, i) + getLong(b, i))
      }
      i += 1
    }
    g
  }

  private def getLong(row: InternalRow, i: Int): Long = {
    if (row.isNullAt(i)) 0L
    else {
      try {
        row.getLong(i)
      } catch {
        case _: Exception =>
          row.get(i, LongType) match {
            case n: java.lang.Number => n.longValue()
            case _ => 0L
          }
      }
    }
  }

  private def getDouble(row: InternalRow, i: Int): Double = {
    if (row.isNullAt(i)) 0.0
    else {
      try {
        row.getDouble(i)
      } catch {
        case _: Exception =>
          row.get(i, DoubleType) match {
            case n: java.lang.Number => n.doubleValue()
            case _ => 0.0
          }
      }
    }
  }

  private def finalizeAgg(
      wide: InternalRow,
      kinds: Seq[NativeAggKind],
      out: Seq[Attribute]): InternalRow = {
    val proj = UnsafeProjection.create(out, out)
    val g = new GenericInternalRow(out.length)
    var wi = 0
    var oi = 0
    while (oi < out.length && wi < kinds.length) {
      kinds(wi) match {
        case NativeAggKind.AvgSum =>
          val sum = getDouble(wide, wi)
          val cnt = if (wi + 1 < kinds.length) getLong(wide, wi + 1) else 0L
          val dt = out(oi).dataType
          if (cnt == 0) g.setNullAt(oi)
          else dt match {
            case DoubleType | FloatType => g.setDouble(oi, sum / cnt)
            case d: DecimalType =>
              val dec = Decimal(sum / cnt)
              g.update(oi, dec)
              val _ = d
            case _ => g.setLong(oi, (sum / cnt).toLong)
          }
          wi += 2
          oi += 1
        case NativeAggKind.AvgCnt =>
          wi += 1
        case NativeAggKind.Pass =>
          if (wide.isNullAt(wi)) {
            g.setNullAt(oi)
          } else {
            setPass(g, oi, out(oi).dataType, wide, wi)
          }
          wi += 1
          oi += 1
        case _ =>
          dtSet(g, oi, out(oi).dataType, wide, wi)
          wi += 1
          oi += 1
      }
    }
    proj(g).copy()
  }

  private def setPass(
      g: GenericInternalRow, oi: Int, dt: DataType, wide: InternalRow, wi: Int): Unit = dt match {
    case StringType =>
      safeGet(wide, wi) match {
        case s: UTF8String => g.update(oi, s)
        case s: String => g.update(oi, UTF8String.fromString(s))
        case n: java.lang.Number =>
          /* hashed string that missed the dict */
          g.update(oi, UTF8String.fromString(String.valueOf(n.longValue())))
        case _ =>
      }
    case other =>
      dtSet(g, oi, other, wide, wi)
  }

  private def dtSet(
      g: GenericInternalRow, oi: Int, dt: DataType, wide: InternalRow, wi: Int): Unit = dt match {
    case DoubleType | FloatType => g.setDouble(oi, getDouble(wide, wi))
    case d: DecimalType =>
      g.update(oi, Decimal(getLong(wide, wi), d.precision, d.scale))
    case LongType | TimestampType => g.setLong(oi, getLong(wide, wi))
    case IntegerType | DateType | ByteType | ShortType => g.setInt(oi, getLong(wide, wi).toInt)
    case _ =>
      if (!wide.isNullAt(wi)) g.update(oi, safeGet(wide, wi))
  }

  private def decodeWide(
      cols: Array[AnyRef],
      n: Int,
      kinds: Seq[NativeAggKind],
      map: java.util.HashMap[java.lang.Long, UTF8String]): Array[InternalRow] = {
    val width = math.max(kinds.length, cols.length)
    val out = new Array[InternalRow](n)
    var r = 0
    while (r < n) {
      val g = new GenericInternalRow(width)
      var c = 0
      while (c < cols.length && c < width) {
        cols(c) match {
          case a: Array[Int] => g.setLong(c, a(r).toLong)
          case a: Array[Long] =>
            val v = a(r)
            val s = map.get(java.lang.Long.valueOf(v))
            if (s != null) g.update(c, s) else g.setLong(c, v)
          case a: Array[Double] => g.setDouble(c, a(r))
          case a: Array[Boolean] => g.setBoolean(c, a(r))
          case _ =>
        }
        c += 1
      }
      out(r) = g
      r += 1
    }
    out
  }

  def evalColumnarDecode(
      rdd: RDD[ColumnarBatch],
      inSchema: Seq[Attribute],
      ir: String,
      outSchema: Seq[Attribute],
      numOutputRows: SQLMetric): RDD[InternalRow] = {
    rdd.mapPartitionsInternal { batches =>
      batches.flatMap { batch =>
        val n = batch.numRows()
        if (n == 0) {
          Iterator.empty
        } else {
          val cols = NativeColumnar.encodeBatch(batch, inSchema)
          val raw = NativeSqlLibrary.execute(ir, Array(cols), Array(n))
          val outN = raw(0).asInstanceOf[Integer].intValue()
          val outCols = raw(1).asInstanceOf[Array[AnyRef]]
          numOutputRows += outN
          decodeRows(outCols, outN, outSchema).iterator
        }
      }
    }
  }

  def evalBroadcastJoin(
      probe: SparkPlan,
      buildIsLeft: Boolean,
      bcast: Broadcast[Array[InternalRow]],
      buildSchema: Seq[Attribute],
      ir: String,
      outSchema: Seq[Attribute],
      probeKey: Int,
      buildKey: Int,
      numOutputRows: SQLMetric): RDD[InternalRow] = {
    val probeSchema = probe.output
    val indexJoin = ir.contains("hashjoinidx")
    val lw = if (buildIsLeft) buildSchema.length else probeSchema.length
    val rw = if (buildIsLeft) probeSchema.length else buildSchema.length
    // Encode build keys once on the driver (Spark BHJ builds the hash table
    // once). Re-encoding every probe partition was the JNI sidecar tax.
    val buildColsOnce =
      if (indexJoin) NativeColumnar.encodeKeyCol(bcast.value, buildSchema, buildKey)
      else encodePartition(buildSchema, bcast.value, withRowId = false)
    val buildColsBcast = probe.session.sparkContext.broadcast(buildColsOnce)
    val buildN = bcast.value.length
    def run(
        probeCols: Array[AnyRef],
        n: Int,
        probeAt: Int => InternalRow): Iterator[InternalRow] = {
      if (n == 0) {
        Iterator.empty
      } else {
        val buildRows = bcast.value
        val buildCols = buildColsBcast.value
        val inputs = new Array[Array[AnyRef]](2)
        if (buildIsLeft) {
          inputs(0) = buildCols
          inputs(1) = probeCols
        } else {
          inputs(0) = probeCols
          inputs(1) = buildCols
        }
        val ns = if (buildIsLeft) Array(buildN, n) else Array(n, buildN)
        val raw = NativeSqlLibrary.execute(ir, inputs, ns)
        val outN = raw(0).asInstanceOf[Integer].intValue()
        val outCols = raw(1).asInstanceOf[Array[AnyRef]]
        numOutputRows += outN
        if (indexJoin) {
          val (li, ri) = joinIndexCols(outCols, outN)
          NativeColumnar.gatherJoin(
            li, ri, outN,
            if (buildIsLeft) (i: Int) => buildRows(i) else probeAt,
            if (buildIsLeft) probeAt else (i: Int) => buildRows(i),
            lw, rw, outSchema).iterator
        } else {
          decodeRows(outCols, outN, outSchema).iterator
        }
      }
    }
    if (probe.supportsColumnar) {
      probe.executeColumnar().mapPartitionsInternal { batches =>
        batches.flatMap { batch =>
          val n = batch.numRows()
          val cols =
            if (indexJoin) NativeColumnar.encodeKeyCol(batch, probeSchema, probeKey)
            else NativeColumnar.encodeBatch(batch, probeSchema)
          run(cols, n, i => batch.getRow(i).copy())
        }
      }
    } else {
      probe.execute().mapPartitionsInternal { iter =>
        val rows = iter.map(_.copy()).toArray
        val cols =
          if (indexJoin) NativeColumnar.encodeKeyCol(rows, probeSchema, probeKey)
          else encodePartition(probeSchema, rows, withRowId = false)
        run(cols, rows.length, i => rows(i))
      }
    }
  }

  private def joinIndexCols(cols: Array[AnyRef], n: Int): (Array[Int], Array[Int]) = {
    def asInt(a: AnyRef): Array[Int] = a match {
      case x: Array[Int] =>
        if (x.length == n) x
        else {
          val o = new Array[Int](n)
          System.arraycopy(x, 0, o, 0, n)
          o
        }
      case x: Array[Long] =>
        val o = new Array[Int](n)
        var i = 0
        while (i < n) {
          o(i) = x(i).toInt
          i += 1
        }
        o
      case _ => Array.empty
    }
    if (cols.length < 2) (Array.empty, Array.empty)
    else (asInt(cols(0)), asInt(cols(1)))
  }

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
      schema: Seq[Attribute],
      strings: java.util.HashMap[java.lang.Long, UTF8String] =
        new java.util.HashMap[java.lang.Long, UTF8String]()): Array[InternalRow] = {
    val proj = UnsafeProjection.create(schema, schema)
    val out = new Array[InternalRow](n)
    var i = 0
    while (i < n) {
      val generic = new GenericInternalRow(schema.length)
      var c = 0
      while (c < schema.length && c < cols.length) {
        (schema(c).dataType, cols(c)) match {
          case (IntegerType | ByteType | ShortType | DateType, a: Array[Int]) =>
            generic.setInt(c, a(i))
          case (IntegerType | ByteType | ShortType | DateType, a: Array[Long]) =>
            generic.setInt(c, a(i).toInt)
          case (LongType | TimestampType, a: Array[Long]) => generic.setLong(c, a(i))
          case (LongType, a: Array[Int]) => generic.setLong(c, a(i).toLong)
          case (DoubleType | FloatType, a: Array[Double]) => generic.setDouble(c, a(i))
          case (BooleanType, a: Array[Boolean]) => generic.setBoolean(c, a(i))
          case (StringType, a: Array[Long]) =>
            val s = strings.get(java.lang.Long.valueOf(a(i)))
            if (s != null) generic.update(c, s)
          case (d: DecimalType, a: Array[Long]) =>
            generic.update(c, Decimal(a(i), d.precision, d.scale))
          case (d: DecimalType, a: Array[Int]) =>
            generic.update(c, Decimal(a(i).toLong, d.precision, d.scale))
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
