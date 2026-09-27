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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{
  Attribute, AttributeSet, BoundReference, Cast, Expression, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{
  AllTuples, ClusteredDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{
  ColumnarToRowExec, FileSourceScanExec, InputAdapter, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.datasources.{FilePartition, FileScanRDD}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DateType, Decimal,
  DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType, StructType}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.vegam.exec.{VegamBackend, VegamPage, VegamTask}
import org.apache.spark.sql.vegam.plan.{CountStar, FileRef, HashAgg, NativePlan, StagePlan}
import org.apache.spark.unsafe.types.UTF8String

/**
 * The only SparkPlan node that talks to a Vegam backend. One convert at the
 * stage edge: backend batches become UnsafeRows.
 *
 * `probeScan` is the Spark scan the stage replaces, kept as a child so that
 * AQE and DPP rules still apply to it. It is never executed for its rows; its
 * FileScanRDD only supplies the task splits (Spark's file ranges after
 * partition pruning), so Vegam reads exactly the ranges Spark would.
 */
case class NativeStageExec(
    nativePlan: NativePlan,
    output: Seq[Attribute],
    backendName: String,
    probeScan: Option[SparkPlan] = None,
    rowChild: Option[SparkPlan] = None)
  extends SparkPlan {

  override def children: Seq[SparkPlan] =
    if (rowChild.isDefined) rowChild.toSeq else probeScan.toSeq

  override def producedAttributes: AttributeSet = outputSet

  /**
   * A shuffle-input stage merges rows that already share a partition.
   * Keep the shuffle Spark placed under this node.
   */
  override def requiredChildDistribution: Seq[Distribution] = nativePlan match {
    case s: StagePlan if rowChild.isDefined => rowDistribution(s)
    case _ => UnspecifiedDistribution :: Nil
  }

  private def rowDistribution(s: StagePlan): Seq[Distribution] = {
    val childOut = rowChild.map(_.output).getOrElse(Nil)
    if (s.groupOrdinals.nonEmpty) {
      val keys = s.groupOrdinals.flatMap { i =>
        if (i >= 0 && i < childOut.length) Some(childOut(i)) else None
      }
      if (keys.length == s.groupOrdinals.length) {
        ClusteredDistribution(keys) :: Nil
      } else {
        UnspecifiedDistribution :: Nil
      }
    } else if (s.aggs.nonEmpty) {
      AllTuples :: Nil
    } else s.window match {
      case Some(w) if w.partitionBy.nonEmpty =>
        val keys = w.partitionBy.flatMap(name => childOut.find(_.name == name))
        if (keys.length == w.partitionBy.length) {
          ClusteredDistribution(keys) :: Nil
        } else {
          UnspecifiedDistribution :: Nil
        }
      case Some(_) => AllTuples :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[SparkPlan]): NativeStageExec = {
    if (rowChild.isDefined) {
      copy(rowChild = newChildren.headOption)
    } else {
      copy(probeScan = newChildren.headOption)
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numTasks" -> SQLMetrics.createMetric(sparkContext, "number of native tasks"),
    "nativeTime" -> SQLMetrics.createNanoTimingMetric(sparkContext, "native time"))

  override def simpleString(maxFields: Int): String = {
    s"NativeStageExec($backendName, ${nativePlan.getClass.getSimpleName})"
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (rowChild.isDefined) {
      executeRows()
    } else {
      executeFiles()
    }
  }

  private def executeRows(): RDD[InternalRow] = {
    val child = rowChild.get
    val plan = nativePlan match {
      case s: StagePlan => s
      case other =>
        throw new IllegalStateException(
          s"vegam: row stage is not a StagePlan (${other.getClass.getSimpleName})")
    }
    val inTypes = child.output.map(_.dataType).toArray
    val targetTypes = output.map(_.dataType).toArray
    val schema = output
    val numOutputRows = longMetric("numOutputRows")
    val nativeTime = longMetric("nativeTime")
    child.execute().mapPartitions { iter =>
      val rows = new ArrayBuffer[Array[Any]]()
      while (iter.hasNext) {
        rows += NativeStageExec.readInput(iter.next(), inTypes)
      }
      val t0 = System.nanoTime()
      val page = org.apache.spark.sql.vegam.exec.JvmBackend.rowStage(plan, rows.toSeq)
      nativeTime += System.nanoTime() - t0
      if (page.numCols != schema.length) {
        throw new IllegalStateException(
          s"vegam: row stage has ${page.numCols} columns, " +
            s"output has ${schema.length}")
      }
      val proj = UnsafeProjection.create(targetTypes)
      val out = (0 until page.numRows).iterator.map { i =>
        numOutputRows += 1
        proj(NativeStageExec.toRow(page, i, schema)).copy()
      }
      out
    }
  }

  private def executeFiles(): RDD[InternalRow] = {
    val plan = nativePlan
    val backend = backendName
    val targetTypes = output.map(_.dataType).toArray
    val schema = output
    val tz = conf.sessionLocalTimeZone
    val splits = NativeStageExec.taskSplits(plan, backend, probeScan)
    val numOutputRows = longMetric("numOutputRows")
    val nativeTime = longMetric("nativeTime")
    longMetric("numTasks") += splits.length
    // The JVM backend reads whole files; it gets each file once, from the
    // split holding the file's first range. libvegam handles ranges itself.
    val wholeFilesOnly = backend == VegamBackend.JVM
    new NativeStageRDD(sparkContext, splits.toArray, (refs: Option[Seq[FileRef]]) => {
      val taskPlan = refs.map { r =>
        plan.withProbeRefs(if (wholeFilesOnly) FileRef.wholeFiles(r) else r)
      }.getOrElse(plan)
      val t0 = System.nanoTime()
      val task = VegamBackend.create(backend).createTask(taskPlan)
      nativeTime += System.nanoTime() - t0
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => task.close()))
      val rows = if (task.isColumnar) {
        NativeStageExec.batchRows(task, () => task.isDone, targetTypes, tz, nativeTime)
      } else {
        // Legacy double pages: materialize before close, as before.
        val proj = UnsafeProjection.create(targetTypes)
        val all = NativeStageExec.pages(task).flatMap { page =>
          if (page.numCols != schema.length) {
            throw new IllegalStateException(
              s"vegam: native page has ${page.numCols} columns, " +
                s"stage output has ${schema.length}")
          }
          (0 until page.numRows).iterator.map { i =>
            proj(NativeStageExec.toRow(page, i, schema)).copy()
          }
        }.toArray
        val rowsToReturn = all.length
        if (!task.isDone) {
          throw new RuntimeException(
            s"vegam: native stage ended without EOF after $rowsToReturn rows" +
              " (set spark.sql.vegam.enabled=false to compare)")
        }
        task.close()
        all.iterator
      }
      rows.map { r =>
        numOutputRows += 1
        r
      }
    })
  }
}

/** One native task: the probe file ranges it reads (None = the plan's own files). */
private[vegam] case class NativeSplit(
    refs: Option[Seq[FileRef]],
    locations: Seq[String]) extends Serializable

private[vegam] class NativeStagePartition(val index: Int, val split: NativeSplit)
  extends Partition

private[vegam] class NativeStageRDD(
    sc: SparkContext,
    @transient private val splits: Array[NativeSplit],
    run: Option[Seq[FileRef]] => Iterator[InternalRow])
  extends RDD[InternalRow](sc, Nil) {

  override protected def getPartitions: Array[Partition] =
    splits.zipWithIndex.map { case (s, i) => new NativeStagePartition(i, s) }

  override protected def getPreferredLocations(p: Partition): Seq[String] =
    p.asInstanceOf[NativeStagePartition].split.locations

  override def compute(p: Partition, context: TaskContext): Iterator[InternalRow] =
    run(p.asInstanceOf[NativeStagePartition].split.refs)
}

object NativeStageExec {

  /**
   * Task splits. With a probe scan, the splits are Spark's own FilePartitions
   * (pruned, sized by spark.sql.files.maxPartitionBytes). A stage that must see
   * the whole input in one task (complete agg, window, expand, shuffle-side
   * join build) gets all ranges in one split.
   */
  private def taskSplits(
      plan: NativePlan,
      backend: String,
      probeScan: Option[SparkPlan]): Seq[NativeSplit] = {
    probeScan.flatMap(filePartitions) match {
      case Some(parts) =>
        val partSchema = probeScan.flatMap(scanNode).map(_.relation.partitionSchema)
          .getOrElse(new StructType())
        val perPart = parts.map { p =>
          NativeSplit(Some(p.files.map(f => toRef(f, partSchema)).toSeq),
            p.preferredLocations().toSeq)
        }
        if (singleTask(plan)) {
          Seq(NativeSplit(Some(perPart.flatMap(_.refs.get)), Nil))
        } else {
          perPart
        }
      case None =>
        legacySplits(plan).map(files => NativeSplit(Some(files), Nil))
    }
  }

  private def singleTask(plan: NativePlan): Boolean = plan match {
    case _: CountStar => true
    case h: HashAgg => h.complete
    // A shuffle-side (SMJ) build re-reads its whole side: splitting the probe
    // would drop the exchange Spark elided (join key == group key).
    case s: StagePlan =>
      s.complete || s.window.isDefined || s.expand.isDefined || s.builds.exists(!_.broadcast)
    case _ => true
  }

  private def legacySplits(plan: NativePlan): Seq[Seq[FileRef]] = {
    val refs: Seq[FileRef] = plan match {
      case s: StagePlan => s.probe.files
      case h: HashAgg => h.refs
      case other => other.files.map(FileRef(_, Nil))
    }
    if (singleTask(plan)) Seq(refs) else refs.map(Seq(_))
  }

  private def scanNode(plan: SparkPlan): Option[FileSourceScanExec] = plan match {
    case s: FileSourceScanExec => Some(s)
    case w: WholeStageCodegenExec => scanNode(w.child)
    case i: InputAdapter => scanNode(i.child)
    case c: ColumnarToRowExec => scanNode(c.child)
    case _ => None
  }

  /**
   * Executing the scan node (not its rows) runs its prepare / subquery wait,
   * so DPP filters are resolved before the FileScanRDD lists partitions.
   */
  private def filePartitions(plan: SparkPlan): Option[Seq[FilePartition]] = {
    scanNode(plan).flatMap { scan =>
      val rdd = if (scan.supportsColumnar) scan.executeColumnar() else scan.execute()
      findFileScan(rdd).map(_.filePartitions)
    }
  }

  private def findFileScan(rdd: RDD[_]): Option[FileScanRDD] = rdd match {
    case f: FileScanRDD => Some(f)
    case other => other.dependencies.iterator.map(d => findFileScan(d.rdd)).collectFirst {
      case Some(f) => f
    }
  }

  private def toRef(
      f: org.apache.spark.sql.execution.datasources.PartitionedFile,
      partSchema: StructType): FileRef = {
    val parts = partSchema.fields.zipWithIndex.flatMap { case (field, i) =>
      if (f.partitionValues.isNullAt(i)) {
        None
      } else {
        Some(field.name -> f.partitionValues.get(i, field.dataType).toString)
      }
    }
    FileRef(VegamPaths.clean(f.filePath.toString), parts.toSeq, f.start, f.length)
  }

  /**
   * Streams typed batches as UnsafeRows. Columns are matched to the output by
   * position; a column whose native type differs from the Spark type is cast
   * with Spark's own Cast.
   */
  private def batchRows(
      task: VegamTask,
      taskGuard: () => Boolean,
      targetTypes: Array[DataType],
      tz: String,
      nativeTime: org.apache.spark.sql.execution.metric.SQLMetric): Iterator[InternalRow] = {
    new Iterator[InternalRow] {
      private var batch: ColumnarBatch = _
      private var row = 0
      private var proj: UnsafeProjection = _
      private var done = false
      private var consumed = 0

      private def advance(): Unit = {
        while (!done && (batch == null || row >= batch.numRows())) {
          val t0 = System.nanoTime()
          val next = task.nextBatch()
          nativeTime += System.nanoTime() - t0
          next match {
            case None =>
              done = true
              batch = null
              task.close()
            case Some(b) =>
              if (b.numCols() != targetTypes.length) {
                throw new IllegalStateException(
                  s"vegam: native batch has ${b.numCols()} columns, " +
                    s"stage output has ${targetTypes.length}")
              }
              if (proj == null) {
                proj = UnsafeProjection.create(castExprs(b, targetTypes, tz))
              }
              batch = b
              row = 0
          }
        }
      }

      override def hasNext: Boolean = {
        advance()
        if (done && !task.isDone) {
          throw new RuntimeException(
            s"vegam: native stage ended without EOF after $consumed rows" +
              " (set spark.sql.vegam.enabled=false to compare)")
        }
        !done
      }

      override def next(): InternalRow = {
        advance()
        if (done) {
          throw new NoSuchElementException("vegam batch rows")
        }
        val r = proj(batch.getRow(row))
        row += 1
        consumed += 1
        r
      }
    }
  }

  private def castExprs(
      b: ColumnarBatch,
      targetTypes: Array[DataType],
      tz: String): Seq[Expression] = {
    val out = new ArrayBuffer[Expression]
    var c = 0
    while (c < targetTypes.length) {
      val src = b.column(c).dataType()
      val ref = BoundReference(c, src, nullable = true)
      out += (if (src == targetTypes(c)) ref else Cast(ref, targetTypes(c), Some(tz)))
      c += 1
    }
    out.toSeq
  }

  private def pages(task: VegamTask): Iterator[VegamPage] = {
    Iterator.continually(task.nextPage()).takeWhile(_.isDefined).map(_.get)
  }

  private def toRow(page: VegamPage, i: Int, schema: Seq[Attribute]): GenericInternalRow = {
    val row = new GenericInternalRow(schema.length)
    var c = 0
    while (c < schema.length && c < page.numCols) {
      if (page.isNull(i, c)) {
        row.setNullAt(c)
      } else {
        val dt = schema(c).dataType
        val text = page.isText(i, c)
        if (dt == StringType && text) {
          row.update(c, UTF8String.fromString(page.getText(i, c)))
        } else if (text && page.getDouble(i, c).isNaN) {
          row.setNullAt(c)
        } else {
          row.update(c, asSpark(page.getDouble(i, c), dt, page.sumScale(c)))
        }
      }
      c += 1
    }
    row
  }

  def readInput(row: InternalRow, types: Array[DataType]): Array[Any] = {
    val out = new Array[Any](types.length)
    var i = 0
    while (i < types.length) {
      out(i) = if (row.isNullAt(i)) {
        null
      } else {
        types(i) match {
          case IntegerType | DateType => row.getInt(i)
          case LongType => row.getLong(i)
          case DoubleType => row.getDouble(i)
          case FloatType => row.getFloat(i)
          case BooleanType => row.getBoolean(i)
          case d: DecimalType =>
            val dec = row.getDecimal(i, d.precision, d.scale)
            if (dec == null) null else Decimal(dec.toJavaBigDecimal)
          case StringType =>
            val s = row.getUTF8String(i)
            if (s == null) null else s.toString
          case other => row.get(i, other)
        }
      }
      i += 1
    }
    out
  }

  def asSpark(value: Double, dt: DataType, unscaledScale: Int = 0): Any = dt match {
    case BooleanType => value != 0.0
    case ByteType => value.toByte
    case ShortType => value.toShort
    case IntegerType => value.toInt
    case LongType if unscaledScale > 0 =>
      math.round(value * math.pow(10.0, unscaledScale.toDouble))
    case LongType => value.toLong
    case FloatType => value.toFloat
    case DoubleType => value
    case DateType => value.toInt
    case d: DecimalType =>
      Decimal(BigDecimal(value), d.precision, d.scale)
    case StringType =>
      UTF8String.fromString(java.lang.Long.toString(value.toLong))
    case _ => value
  }
}
