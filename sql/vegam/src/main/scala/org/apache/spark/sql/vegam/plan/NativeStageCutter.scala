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

package org.apache.spark.sql.vegam.plan

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.execution.{
  ColumnarToRowExec, EmptyRelationExec, ExpandExec, FileSourceScanExec, FilterExec,
  InputAdapter, ProjectExec, SortExec, SparkPlan, UnionExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, QueryStageExec}
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType, LongType, StringType}
import org.apache.spark.sql.vegam.VegamPaths
import org.apache.spark.unsafe.types.UTF8String

import scala.util.{Either, Left, Right}

/**
 * Lowers one Spark stage to [[NativePlan]] or rejects it. Never walks
 * [[QueryStageExec]]: a later AQE Final must not re-read the original files.
 */
object NativeStageCutter {

  sealed trait CutResult
  /**
   * `probeScan` is the Spark scan feeding the probe side, when there is exactly one.
   * NativeStageExec keeps it as a child and takes its task splits from it, so
   * static partition pruning, DPP and Spark's split sizing all apply.
   */
  case class CutOk(
      plan: NativePlan,
      probeScan: Option[SparkPlan] = None,
      rowChild: Option[SparkPlan] = None) extends CutResult
  case class CutSkip(why: String, detail: String) extends CutResult

  def cut(plan: SparkPlan): CutResult = unwrap(plan) match {
    case agg: HashAggregateExec => cutAgg(agg)
    case agg: SortAggregateExec => cutAgg(agg)
    case w: WindowExec => cutWindow(w)
    case other => CutSkip("unsupported-root", other.nodeName)
  }

  private def cutAgg(agg: BaseAggregateExec): CutResult = {
    if (rowInput(agg.child)) {
      // The child is a shuffle or an AQE shuffle read. Read those rows.
      // Do not walk through them back to the files.
      cutRowStage(agg)
    } else if (isCountStar(agg) && !hasJoin(agg) && !hasWindow(agg)) {
      cutCount(agg)
    } else {
      cutStage(agg)
    }
  }

  private def cutCount(agg: BaseAggregateExec): CutResult = {
    lowerPipeline(agg.child) match {
      case Left(skip) => skip
      case Right(pipe) if pipe.builds.nonEmpty || pipe.window.isDefined =>
        cutStage(agg)
      case Right(pipe) =>
        val extras = pipe.probeFilters ++ pipe.scanFilters
        if (extras.nonEmpty) {
          cutStage(agg)
        } else if (pipe.probe.files.isEmpty) {
          CutSkip("no-files", "count")
        } else {
          CutOk(CountStar(pipe.probe.paths.map(VegamPaths.clean)), pipe.probeNode)
        }
    }
  }

  private def cutStage(agg: BaseAggregateExec): CutResult = {
    val parsed = parseAggs(agg)
    if (!parsed.ok) {
      return CutSkip(parsed.why, parsed.detail)
    }
    if (agg.aggregateExpressions.exists(e => e.mode != Partial && e.mode != Complete)) {
      return CutSkip("agg-mode", agg.aggregateExpressions.map(_.mode).mkString(","))
    }
    lowerPipeline(agg.child) match {
      case Left(skip) => skip
      case Right(pipe) if ambiguous(pipe).nonEmpty =>
        CutSkip("ambiguous-column", ambiguous(pipe).mkString(","))
      case Right(pipe) =>
        val complete = agg.aggregateExpressions.headOption.exists(_.mode == Complete)
        val width = parsed.groups.length + parsed.aggs.map(_.buffers).sum
        val layout = if (complete) bindOutput(agg, divide = false) else Left("")
        val reorder = layout.toOption.filterNot(identityLayout)
        if (agg.output.length != width && reorder.isEmpty) {
          CutSkip("agg-width", s"$width != ${agg.output.length}")
        } else if (reorder.isEmpty && isSimpleGroupSum(agg, pipe)) {
          CutOk(toHashAgg(agg, pipe, complete), pipe.probeNode)
        } else {
            val (at, div) = reorder.map(cols => (cols.map(_._1), cols.map(_._2)))
              .getOrElse((Nil, Nil))
            CutOk(StagePlan(
              probe = pipe.probe,
              builds = pipe.builds,
              probeFilters = pipe.probeFilters ++ pipe.scanFilters,
              groups = parsed.groups,
              groupTypes = parsed.groupTypes,
              aggs = parsed.aggs,
              window = pipe.window,
              complete = complete,
              expand = pipe.expand,
              projects = pipe.projects,
              resultAt = at,
              resultDiv = div), pipe.probeNode)
        }
    }
  }

  private def cutWindow(w: WindowExec): CutResult = {
    parseWindow(w) match {
      case Left(skip) => skip
      case Right(spec) if rowInput(w.child) =>
        val names = w.child.output.map(_.name)
        val types = w.child.output.map(_.dataType.simpleString)
        if (spec.partitionBy.exists(c => !names.contains(c)) ||
            spec.orderBy.exists { case (c, _) => !names.contains(c) } ||
            spec.fns.exists(f => f.col.nonEmpty && !names.contains(f.col))) {
          CutSkip("window-fn", "row")
        } else {
          CutOk(StagePlan(
            probe = ScanSpec(Nil, names, types),
            builds = Nil,
            probeFilters = Nil,
            groups = Nil,
            groupTypes = Nil,
            aggs = Nil,
            window = Some(spec),
            complete = true,
            rowSource = true), None, Some(w.child))
        }
      case Right(spec) =>
        lowerPipeline(w.child) match {
          case Left(skip) => skip
          case Right(pipe) if pipe.window.isDefined =>
            CutSkip("nested-window", "window")
          case Right(pipe) if ambiguous(pipe).nonEmpty =>
            CutSkip("ambiguous-column", ambiguous(pipe).mkString(","))
          case Right(pipe) =>
            CutOk(StagePlan(
              probe = pipe.probe,
              builds = pipe.builds,
              probeFilters = pipe.probeFilters ++ pipe.scanFilters,
              groups = Nil,
              groupTypes = Nil,
              aggs = Nil,
              window = Some(spec),
              complete = true,
              expand = pipe.expand,
              projects = pipe.projects), pipe.probeNode)
        }
    }
  }

  private case class Pipe(
      probe: ScanSpec,
      builds: Seq[BuildJoin],
      probeFilters: Seq[FilterPred],
      scanFilters: Seq[FilterPred],
      window: Option[WinSpec],
      expand: Option[ExpandSpec] = None,
      probeNode: Option[SparkPlan] = None,
      projects: Seq[NamedExpr] = Nil)

  /**
   * Column names are the only join-side identity in the IR, so a name that
   * appears on more than one joined scan (e.g. date_dim joined twice) is
   * ambiguous. Such stages stay on Spark. Semi and anti builds emit no
   * columns, and an inner-join key named the same on both sides carries the
   * same value on either side, so neither makes a name ambiguous.
   */
  private def ambiguous(pipe: Pipe): Seq[String] = {
    val emitting = pipe.builds.filter(b =>
      b.joinType == NativePlan.JOIN_INNER || b.joinType == NativePlan.JOIN_LEFT)
    val sharedKeys = emitting.filter(_.joinType == NativePlan.JOIN_INNER).flatMap { b =>
      b.probeKeys.zip(b.buildKeys).collect { case (p, q) if p == q => p }
    }.toSet
    val scans = pipe.probe +: emitting.map(_.scan)
    scans.flatMap(_.columns.distinct).filterNot(sharedKeys).groupBy(identity).collect {
      case (name, seen) if seen.length > 1 => name
    }.toSeq.sorted
  }

  private def lowerPipeline(plan: SparkPlan): Either[CutSkip, Pipe] = {
    unwrap(plan) match {
      case _: QueryStageExec =>
        Left(CutSkip("query-stage", "pipeline"))
      case e: Exchange =>
        // Native stage re-reads parquet; ignore the shuffle wrapper.
        lowerPipeline(e.child)
      case s: SortExec =>
        lowerPipeline(s.child)
      case e: ExpandExec =>
        parseExpand(e) match {
          case Left(skip) => Left(skip)
          case Right(spec) =>
            lowerPipeline(e.child).flatMap { p =>
              if (p.expand.isDefined) {
                Left(CutSkip("nested-expand", "expand"))
              } else {
                Right(p.copy(expand = Some(spec)))
              }
            }
        }
      case u: UnionExec =>
        lowerUnion(u)
      case j: BroadcastHashJoinExec =>
        lowerJoin(j)
      case j: SortMergeJoinExec =>
        lowerSortMerge(j)
      case w: WindowExec =>
        parseWindow(w) match {
          case Left(skip) => Left(skip)
          case Right(spec) =>
            lowerPipeline(w.child).map(p => p.copy(window = Some(spec)))
        }
      case f: FilterExec =>
        parseBool(f.condition) match {
          case Left(skip) => Left(skip)
          case Right(preds) =>
            lowerPipeline(f.child).map(p => p.copy(probeFilters = p.probeFilters ++ preds))
        }
      case p: ProjectExec =>
        lowerProject(p)
      case e: EmptyRelationExec =>
        Right(Pipe(
          probe = ScanSpec(Nil, e.output.map(_.name), e.output.map(_.dataType.simpleString)),
          builds = Nil,
          probeFilters = Nil,
          scanFilters = Nil,
          window = None,
          probeNode = Some(e)))
      case s: FileSourceScanExec =>
        scanToPipe(v1Scan(s)).map(_.copy(probeNode = Some(s)))
      case b: BatchScanExec =>
        v2Scan(b) match {
          case None => Left(CutSkip("v2-scan", b.scan.getClass.getName))
          case Some(info) => scanToPipe(info)
        }
      case a: BaseAggregateExec =>
        Left(CutSkip("nested-agg", a.nodeName))
      case other if other.nodeName.startsWith("NativeStage") =>
        Left(CutSkip("nested-native", other.nodeName))
      case other if other.children.size == 1 =>
        lowerPipeline(other.children.head)
      case other =>
        Left(CutSkip("unsupported-node", other.nodeName))
    }
  }

  private def lowerUnion(u: UnionExec): Either[CutSkip, Pipe] = {
    val kids = u.children.map(lowerPipeline)
    kids.collectFirst { case Left(s) => s } match {
      case Some(skip) => Left(skip)
      case None =>
        val pipes = kids.collect { case Right(p) => p }
        if (pipes.exists(p => p.builds.nonEmpty || p.window.isDefined || p.expand.isDefined)) {
          Left(CutSkip("union-join", "union"))
        } else {
          val files = pipes.flatMap(_.probe.files)
          val cols = pipes.headOption.map(_.probe.columns).getOrElse(Nil)
          Right(Pipe(
            probe = ScanSpec(files, cols),
            builds = Nil,
            probeFilters = pipes.flatMap(_.probeFilters),
            scanFilters = pipes.flatMap(_.scanFilters),
            window = None,
            probeNode = None))
        }
    }
  }

  private def lowerJoin(j: BroadcastHashJoinExec): Either[CutSkip, Pipe] = {
    if (j.condition.isDefined) {
      return Left(CutSkip("join-cond", j.joinType.sql))
    }
    if (j.isNullAwareAntiJoin) {
      return Left(CutSkip("null-aware-anti", j.joinType.sql))
    }
    joinKind(j.joinType) match {
      case None => Left(CutSkip("join-type", j.joinType.sql))
      case Some(kind) =>
        val (probeP, buildP, pKeys, bKeys) = j.buildSide match {
          case BuildRight => (j.left, j.right, j.leftKeys, j.rightKeys)
          case BuildLeft => (j.right, j.left, j.rightKeys, j.leftKeys)
        }
        val pk = pKeys.flatMap(keyExpr)
        val bk = bKeys.flatMap(keyExpr)
        if (pk.length != pKeys.length || bk.length != bKeys.length) {
          return Left(CutSkip("join-keys", j.nodeName))
        }
        for {
          probe <- lowerPipeline(probeP)
          build <- lowerPipeline(buildP)
        } yield {
          val bj = BuildJoin(
            scan = build.probe,
            probeKeys = pk,
            buildKeys = bk,
            joinType = kind,
            filters = build.probeFilters ++ build.scanFilters)
          probe.copy(builds = probe.builds ++ build.builds :+ bj)
        }
    }
  }

  private def lowerSortMerge(j: SortMergeJoinExec): Either[CutSkip, Pipe] = {
    if (j.condition.isDefined) {
      return Left(CutSkip("smj-cond", j.joinType.sql))
    }
    joinKind(j.joinType) match {
      case None => Left(CutSkip("join-type", j.joinType.sql))
      case Some(kind) =>
        val pk = j.leftKeys.flatMap(keyExpr)
        val bk = j.rightKeys.flatMap(keyExpr)
        if (pk.length != j.leftKeys.length || bk.length != j.rightKeys.length) {
          return Left(CutSkip("join-keys", "smj"))
        }
        for {
          left <- lowerPipeline(j.left)
          right <- lowerPipeline(j.right)
        } yield {
          val bj = BuildJoin(
            scan = right.probe,
            probeKeys = pk,
            buildKeys = bk,
            joinType = kind,
            filters = right.probeFilters ++ right.scanFilters,
            broadcast = false)
          left.copy(builds = left.builds ++ right.builds :+ bj)
        }
    }
  }

  private def joinKind(t: JoinType): Option[Int] = t match {
    case _: InnerLike => Some(NativePlan.JOIN_INNER)
    case LeftOuter => Some(NativePlan.JOIN_LEFT)
    case LeftSemi => Some(NativePlan.JOIN_SEMI)
    case LeftAnti => Some(NativePlan.JOIN_ANTI)
    case _ => None
  }

  private def scanToPipe(scan: ScanInfo): Either[CutSkip, Pipe] = {
    parquetFiles(scan) match {
      case CutSkip(why, detail) => Left(CutSkip(why, detail))
      case _: CutOk =>
        val parsed = scan.dataFilters.map(parseBool)
        if (parsed.exists(_.isLeft)) {
          Left(parsed.collectFirst { case Left(skip) => skip }.get)
        } else {
          Right(Pipe(
            probe = ScanSpec(scan.refs, scan.columns, scan.types),
            builds = Nil,
            probeFilters = Nil,
            scanFilters = parsed.collect { case Right(ps) => ps }.flatten,
            window = None))
        }
      case _ =>
        Left(CutSkip("internal", "scan"))
    }
  }

  private case class ScanInfo(
      refs: Seq[FileRef],
      partitionSchemaEmpty: Boolean,
      parquet: Boolean,
      dataFilters: Seq[Expression],
      columns: Seq[String],
      detail: String,
      types: Seq[String] = Nil)

  private def unwrap(plan: SparkPlan): SparkPlan = plan match {
    case w: WholeStageCodegenExec => unwrap(w.child)
    case i: InputAdapter => unwrap(i.child)
    case c: ColumnarToRowExec => unwrap(c.child)
    case other => other
  }

  private def v1Scan(scan: FileSourceScanExec): ScanInfo = {
    val index = scan.relation.location
    val partSchema = scan.relation.partitionSchema
    val refs = fileRefs(index, partSchema)
    ScanInfo(
      refs = refs,
      partitionSchemaEmpty = partSchema.isEmpty,
      parquet = scan.relation.fileFormat.isInstanceOf[ParquetFileFormat],
      dataFilters = scan.dataFilters,
      columns = scan.output.map(_.name),
      detail = index.toString,
      types = scan.output.map(_.dataType.simpleString))
  }

  private def v2Scan(scan: BatchScanExec): Option[ScanInfo] = scan.scan match {
    case p: ParquetScan =>
      Some(fromFileIndex(p.fileIndex, p.dataFilters, parquet = true, scan.output.map(_.name),
        "parquet-v2").copy(types = scan.output.map(_.dataType.simpleString)))
    case f: FileScan =>
      Some(fromFileIndex(f.fileIndex, f.dataFilters, parquet = false, scan.output.map(_.name),
        f.getClass.getName))
    case _ =>
      None
  }

  private def fromFileIndex(
      index: PartitioningAwareFileIndex,
      dataFilters: Seq[Expression],
      parquet: Boolean,
      columns: Seq[String],
      detail: String): ScanInfo = {
    val partSchema = index.partitionSchema
    ScanInfo(
      refs = fileRefs(index, partSchema),
      partitionSchemaEmpty = partSchema.isEmpty,
      parquet = parquet,
      dataFilters = dataFilters,
      columns = columns,
      detail = detail)
  }

  private def fileRefs(
      index: FileIndex,
      partSchema: org.apache.spark.sql.types.StructType): Seq[FileRef] = {
    if (partSchema.isEmpty) {
      index.inputFiles.toSeq.map(p => FileRef(VegamPaths.clean(p), Nil))
    } else {
      index.listFiles(Nil, Nil).flatMap { pd =>
        val pairs = partSchema.zipWithIndex.flatMap { case (field, i) =>
          partValue(pd.values, i, field.dataType).map(field.name -> _)
        }
        pd.files.map { f =>
          FileRef(VegamPaths.clean(f.getPath.toString), pairs.toSeq)
        }
      }
    }
  }

  private def partValue(row: InternalRow, i: Int, dt: org.apache.spark.sql.types.DataType)
      : Option[String] = {
    if (row.isNullAt(i)) {
      None
    } else {
      dt match {
        case StringType => Some(row.getUTF8String(i).toString)
        case org.apache.spark.sql.types.IntegerType => Some(row.getInt(i).toString)
        case LongType => Some(row.getLong(i).toString)
        case d: DecimalType => Some(row.getDecimal(i, d.precision, d.scale).toString)
        case _ => Some(row.get(i, dt).toString)
      }
    }
  }

  private def parquetFiles(scan: ScanInfo): CutResult = {
    if (!scan.parquet) {
      return CutSkip("not-parquet", scan.detail)
    }
    val files = scan.refs.map(_.path)
    if (files.isEmpty) {
      CutSkip("no-files", scan.detail)
    } else if (!files.forall(VegamPaths.isSupported)) {
      CutSkip("unsupported-fs", files.take(3).mkString(","))
    } else {
      CutOk(CountStar(files))
    }
  }

  private def isCountStar(agg: BaseAggregateExec): Boolean = {
    agg.groupingExpressions.isEmpty &&
      agg.aggregateExpressions.length == 1 && {
        val e = agg.aggregateExpressions.head
        e.aggregateFunction.isInstanceOf[Count] &&
          (e.mode == Final || e.mode == Complete || e.mode == Partial)
      }
  }

  private def isSimpleGroupSum(agg: BaseAggregateExec, pipe: Pipe): Boolean = {
    pipe.builds.isEmpty &&
      pipe.window.isEmpty &&
      pipe.expand.isEmpty &&
      pipe.projects.isEmpty &&
      agg.groupingExpressions.length == 1 &&
      agg.aggregateExpressions.length == 1 &&
      agg.aggregateExpressions.head.aggregateFunction.isInstanceOf[Sum] &&
      agg.aggregateExpressions.head.aggregateFunction.aggBufferAttributes.length == 1
  }

  private def toHashAgg(agg: BaseAggregateExec, pipe: Pipe, complete: Boolean): HashAgg = {
    val e = agg.aggregateExpressions.head
    val filters = pipe.probeFilters ++ pipe.scanFilters
    HashAgg(
      files = pipe.probe.paths,
      groupCol = leafName(agg.groupingExpressions.head).get,
      sumCol = sumName(agg).get,
      filter = filters.headOption,
      sumScale = sumScale(agg),
      groupType = agg.groupingExpressions.head.dataType,
      sumType = agg.aggregateAttributes.headOption.map(_.dataType)
        .getOrElse(e.aggregateFunction.dataType),
      complete = complete,
      groupCols = leafName(agg.groupingExpressions.head).toSeq,
      aggs = Seq(AggCall(NativePlan.AGG_SUM, sumName(agg).get, sumScale(agg),
        agg.aggregateAttributes.headOption.map(_.dataType)
          .getOrElse(e.aggregateFunction.dataType))),
      filters = filters,
      fileRefs = pipe.probe.files)
  }

  private case class AggParse(
      ok: Boolean,
      why: String,
      detail: String,
      groups: Seq[String],
      groupTypes: Seq[org.apache.spark.sql.types.DataType],
      aggs: Seq[AggCall])

  private def parseAggs(agg: BaseAggregateExec): AggParse = {
    val groups = agg.groupingExpressions.flatMap(leafName)
    if (groups.length != agg.groupingExpressions.length) {
      return AggParse(false, "group-expr", "", Nil, Nil, Nil)
    }
    val calls = agg.aggregateExpressions.flatMap(toAggCall)
    if (calls.length != agg.aggregateExpressions.length) {
      return AggParse(false, "unsupported-agg",
        agg.aggregateExpressions.map(_.aggregateFunction.prettyName).mkString(","),
        Nil, Nil, Nil)
    }
    AggParse(true, "", "", groups, agg.groupingExpressions.map(_.dataType), calls)
  }

  private def toAggCall(e: AggregateExpression): Option[AggCall] = {
    val dt = e.aggregateFunction.dataType
    val built = e.aggregateFunction match {
      case _: Count if e.aggregateFunction.children.isEmpty ||
          e.aggregateFunction.prettyName == "count" =>
        val star = e.aggregateFunction.children.isEmpty ||
          e.aggregateFunction.children.exists(_.isInstanceOf[Literal])
        if (star) {
          Some(AggCall(NativePlan.AGG_COUNT_STAR, "", 0, LongType))
        } else {
          val c = e.aggregateFunction.children.head
          aggOf(NativePlan.AGG_COUNT, c, 0, LongType)
        }
      case c: Count =>
        val child = c.children.headOption
        if (child.exists(_.isInstanceOf[Literal])) {
          Some(AggCall(NativePlan.AGG_COUNT_STAR, "", 0, LongType))
        } else {
          child.flatMap(ch => aggOf(NativePlan.AGG_COUNT, ch, 0, LongType))
        }
      case s: Sum => aggOf(NativePlan.AGG_SUM, s.child, exprScale(s.child), dt)
      case m: Min => aggOf(NativePlan.AGG_MIN, m.child, exprScale(m.child), dt)
      case m: Max => aggOf(NativePlan.AGG_MAX, m.child, exprScale(m.child), dt)
      case a: Average => aggOf(NativePlan.AGG_AVG, a.child, exprScale(a.child), dt)
      case _ => None
    }
    val buffers = e.mode match {
      case Partial | PartialMerge => e.aggregateFunction.aggBufferAttributes.length
      case _ => 1
    }
    built.map(_.copy(mode = modeTag(e.mode), buffers = buffers))
  }

  private def aggOf(
      kind: Int,
      child: Expression,
      scale: Int,
      dt: DataType): Option[AggCall] = {
    // Keep the encodings Velox already evaluates, and use a VExpr only when
    // the input is not a bare column, an unscaled decimal, or a cast of one.
    aggInput(child) match {
      case NativePlan.INPUT_UNSUPPORTED =>
        VExpr.parse(child).map { ex =>
          val col = VExpr.firstCol(ex).getOrElse("")
          AggCall(kind, col, scale, dt, VExpr.encode(ex))
        }
      case input =>
        leafName(child).map(n => AggCall(kind, n, scale, dt, input))
    }
  }

  private def modeTag(mode: AggregateMode): Int = mode match {
    case Final => NativePlan.MODE_FINAL
    case PartialMerge => NativePlan.MODE_MERGE
    case Partial => NativePlan.MODE_PARTIAL
    case Complete => NativePlan.MODE_UPDATE
  }

  /** How the aggregate input is derived from its column; see AggCall.input. */
  private def aggInput(e: Expression): String = e match {
    case Alias(c, _) => aggInput(c)
    case _: Attribute => ""
    case UnscaledValue(_: Attribute) => NativePlan.INPUT_UNSCALED
    case c: Cast if c.child.isInstanceOf[Attribute] =>
      NativePlan.INPUT_CAST_PREFIX + c.dataType.simpleString
    case _ => NativePlan.INPUT_UNSUPPORTED
  }

  private def parseExpand(e: ExpandExec): Either[CutSkip, ExpandSpec] = {
    val names = e.output.map(_.name)
    val projs = e.projections.map(_.map(parseExpandSlot))
    if (projs.exists(_.exists(_.isEmpty))) {
      Left(CutSkip("expand-expr", e.projections.flatten.map(_.prettyName).distinct.mkString(",")))
    } else if (projs.exists(_.length != names.length)) {
      Left(CutSkip("expand-width", e.nodeName))
    } else {
      Right(ExpandSpec(names, projs.map(_.flatten)))
    }
  }

  private def parseExpandSlot(e: Expression): Option[ExpandSlot] = e match {
    case a: Attribute => Some(ExpandSlot(NativePlan.EXPAND_COL, a.name))
    case Alias(c, _) => parseExpandSlot(c)
    case c: Cast => parseExpandSlot(c.child)
    case Literal(null, _) => Some(ExpandSlot(NativePlan.EXPAND_NULL))
    case Literal(v, _) => v match {
      case i: java.lang.Integer => Some(ExpandSlot(NativePlan.EXPAND_LONG, lvalue = i.longValue()))
      case l: java.lang.Long => Some(ExpandSlot(NativePlan.EXPAND_LONG, lvalue = l.longValue()))
      case i: Int => Some(ExpandSlot(NativePlan.EXPAND_LONG, lvalue = i.toLong))
      case l: Long => Some(ExpandSlot(NativePlan.EXPAND_LONG, lvalue = l))
      case d: java.lang.Double => Some(ExpandSlot(NativePlan.EXPAND_DOUBLE, dvalue = d.doubleValue()))
      case f: java.lang.Float => Some(ExpandSlot(NativePlan.EXPAND_DOUBLE, dvalue = f.doubleValue()))
      case dec: Decimal => Some(ExpandSlot(NativePlan.EXPAND_DOUBLE, dvalue = dec.toDouble))
      case s: UTF8String => Some(ExpandSlot(NativePlan.EXPAND_STR, svalue = s.toString))
      case s: String => Some(ExpandSlot(NativePlan.EXPAND_STR, svalue = s))
      case other => toLong(other).map(n => ExpandSlot(NativePlan.EXPAND_LONG, lvalue = n))
    }
    case _ => None
  }

  private def parseWindow(w: WindowExec): Either[CutSkip, WinSpec] = {
    val part = w.partitionSpec.flatMap(leafName)
    if (part.length != w.partitionSpec.length) {
      return Left(CutSkip("window-part", w.nodeName))
    }
    val order = w.orderSpec.flatMap { s =>
      leafName(s.child).map(_ -> s.isAscending)
    }
    if (order.length != w.orderSpec.length) {
      return Left(CutSkip("window-order", w.nodeName))
    }
    val fns = w.windowExpression.flatMap(toWindowCall)
    if (fns.length != w.windowExpression.length) {
      return Left(CutSkip("window-fn",
        w.windowExpression.map(_.prettyName).mkString(",")))
    }
    Right(WinSpec(part, order, fns))
  }

  private def toWindowCall(e: NamedExpression): Option[WindowCall] = {
    val alias = e.name
    val inner = e match {
      case Alias(c, _) => c
      case other => other
    }
    inner match {
      case WindowExpression(fn, spec) =>
        fn match {
          case _: RowNumber => Some(WindowCall(NativePlan.WIN_ROW_NUMBER, "", alias))
          case _: Rank => Some(WindowCall(NativePlan.WIN_RANK, "", alias))
          case _: DenseRank => Some(WindowCall(NativePlan.WIN_DENSE_RANK, "", alias))
          case a: AggregateExpression =>
            val frame = frameOf(spec)
            a.aggregateFunction match {
              case s: Sum if frame >= 0 =>
                leafName(s.child).map(n =>
                  WindowCall(NativePlan.WIN_SUM, n, alias, frame))
              case m: Min if frame >= 0 =>
                leafName(m.child).map(n =>
                  WindowCall(NativePlan.WIN_MIN, n, alias, frame))
              case m: Max if frame >= 0 =>
                leafName(m.child).map(n =>
                  WindowCall(NativePlan.WIN_MAX, n, alias, frame))
              case _ => None
            }
          case _ => None
        }
      case _ => None
    }
  }

  private def frameOf(spec: Any): Int = spec match {
    case w: WindowSpecDefinition => frameOf(w.frameSpecification)
    case SpecifiedWindowFrame(RowFrame, UnboundedPreceding, CurrentRow) =>
      NativePlan.FRAME_RUNNING
    case SpecifiedWindowFrame(RowFrame, UnboundedPreceding, UnboundedFollowing) =>
      NativePlan.FRAME_PARTITION
    case UnspecifiedFrame => NativePlan.FRAME_PARTITION
    case _ => -1
  }

  private def parseBool(e: Expression): Either[CutSkip, Seq[FilterPred]] = {
    val parts = splitOr(e)
    if (parts.length == 1) {
      parseFilters(flattenFilters(parts.head))
    } else {
      val groups = parts.zipWithIndex.map { case (p, i) =>
        parseFilters(flattenFilters(p)).map(_.map(_.copy(orGroup = i + 1)))
      }
      if (groups.exists(_.isLeft)) {
        Left(groups.collectFirst { case Left(skip) => skip }.get)
      } else {
        Right(groups.collect { case Right(ps) => ps }.flatten)
      }
    }
  }

  private def splitOr(e: Expression): Seq[Expression] = e match {
    case Or(l, r) => splitOr(l) ++ splitOr(r)
    case other => Seq(other)
  }

  private def lowerProject(p: ProjectExec): Either[CutSkip, Pipe] = {
    if (p.projectList.forall(e => leafName(e).isDefined)) {
      lowerPipeline(p.child)
    } else {
      val named = p.projectList.map { e =>
        val child = e match {
          case Alias(c, _) => c
          case other => other
        }
        VExpr.parse(child).map(ex => NamedExpr(e.name, VExpr.encode(ex)))
      }
      if (named.forall(_.isDefined)) {
        lowerPipeline(p.child).map { pipe =>
          pipe.copy(projects = pipe.projects ++ named.flatten)
        }
      } else {
        Left(CutSkip("project-expr", p.projectList.map(_.prettyName).mkString(",")))
      }
    }
  }

  private def keyExpr(e: Expression): Option[String] = {
    leafName(e).orElse(VExpr.parse(e).map(VExpr.encode))
  }

  /**
   * True when this plan's rows come from a shuffle. Stop here.
   * Do not walk a query stage back to the files.
   */
  private def rowInput(plan: SparkPlan): Boolean = unwrap(plan) match {
    case _: Exchange => true
    case _: QueryStageExec => true
    case _: AQEShuffleReadExec => true
    case p: ProjectExec => rowInput(p.child)
    case f: FilterExec => rowInput(f.child)
    case s: SortExec => rowInput(s.child)
    case _ => false
  }

  /**
   * Aggregate or distinct over shuffle rows. Group keys and buffers are
   * ordinals into the child row, so duplicate buffer names (sum, count)
   * stay distinct. A failure leaves the aggregate on Spark.
   */
  private def cutRowStage(agg: BaseAggregateExec): CutResult = {
    val out = agg.child.output
    val groupOrds = agg.groupingExpressions.map {
      case a: Attribute =>
        val byId = out.indexWhere(_.exprId == a.exprId)
        if (byId >= 0) byId else out.indexWhere(_.name == a.name)
      case _ => -1
    }
    if (groupOrds.exists(_ < 0)) {
      return CutSkip("shuffled-agg", agg.nodeName)
    }
    if (agg.aggregateExpressions.isEmpty) {
      if (agg.output.length != groupOrds.length) {
        return CutSkip("shuffled-agg", "distinct-width")
      }
      return rowAggOk(agg, groupOrds, Nil, Nil)
    }
    val modes = agg.aggregateExpressions.map(_.mode).distinct
    if (modes.length != 1) {
      return CutSkip("agg-mode", modes.mkString(","))
    }
    val calls = agg.aggregateExpressions.flatMap(toAggCall)
    if (calls.length != agg.aggregateExpressions.length) {
      return CutSkip("unsupported-agg",
        agg.aggregateExpressions.map(_.aggregateFunction.prettyName).mkString(","))
    }
    val ords: Seq[Seq[Int]] = modes.head match {
      case Final | PartialMerge =>
        val widths = agg.aggregateExpressions.map(_.aggregateFunction.aggBufferAttributes.length)
        if (out.length != groupOrds.length + widths.sum) {
          return CutSkip("shuffled-agg", "buffer-width")
        }
        var off = groupOrds.length
        widths.map { w =>
          val xs = (off until off + w).toSeq
          off += w
          xs
        }
      case Partial =>
        calls.map { c =>
          if (c.input.startsWith("(") || c.col.isEmpty) {
            Nil
          } else {
            val i = out.indexWhere(_.name == c.col)
            if (i < 0) return CutSkip("shuffled-agg", c.col)
            Seq(i)
          }
        }
      case _ =>
        return CutSkip("agg-mode", modes.head.toString)
    }
    val finalMode = modes.head == Final
    val layout = if (finalMode) {
      bindOutput(agg, divide = true) match {
        case Left(detail) => return CutSkip("result-expr", detail)
        case Right(cols) => cols
      }
    } else {
      Nil
    }
    val width = groupOrds.length + calls.map { c =>
      if (c.mode == NativePlan.MODE_FINAL) 1 else c.buffers
    }.sum
    if (!finalMode && width != agg.output.length) {
      return CutSkip("shuffled-agg", s"out $width != ${agg.output.length}")
    }
    rowAggOk(agg, groupOrds, calls, ords, layout)
  }

  private def rowAggOk(
      agg: BaseAggregateExec,
      groupOrds: Seq[Int],
      calls: Seq[AggCall],
      ords: Seq[Seq[Int]],
      layout: Seq[(Int, Int)] = Nil): CutResult = {
    val child = agg.child
    CutOk(StagePlan(
      probe = ScanSpec(Nil, child.output.map(_.name),
        child.output.map(_.dataType.simpleString)),
      builds = Nil,
      probeFilters = Nil,
      groups = groupOrds.map(i => child.output(i).name),
      groupTypes = groupOrds.map(i => child.output(i).dataType),
      aggs = calls,
      window = None,
      complete = false,
      rowSource = true,
      groupOrdinals = groupOrds,
      aggOrdinals = ords,
      resultAt = layout.map(_._1),
      resultDiv = layout.map(_._2)), None, Some(child))
  }

  /**
   * Map each result expression onto the row this stage builds
   * (group values, then one column per aggregate). `divide` is set for a
   * final aggregate, whose buffers are unscaled longs that MakeDecimal
   * turns back into decimals. A complete file-stage sum already holds the
   * scaled value, so it only reorders.
   */
  private def bindOutput(
      agg: BaseAggregateExec,
      divide: Boolean): Either[String, Seq[(Int, Int)]] = {
    val nGroups = agg.groupingExpressions.length
    val groupIds = agg.groupingExpressions.flatMap(attrId)
    val aggIds = agg.aggregateAttributes.map(_.exprId)
    if (divide && aggIds.length != agg.aggregateExpressions.length) {
      return Left("agg-attrs")
    }
    def locate(a: Attribute): Option[Int] = {
      val g = groupIds.indexOf(a.exprId)
      if (g >= 0) {
        Some(g)
      } else {
        val i = aggIds.indexOf(a.exprId)
        if (i >= 0) Some(nGroups + i) else None
      }
    }
    def peel(e: Expression): Option[(Int, Int)] = e match {
      case a: Attribute => locate(a).map(i => (i, 0))
      case Alias(c, _) => peel(c)
      case KnownNotNull(c) => peel(c)
      case c: CheckOverflow => peel(c.child)
      case m: MakeDecimal =>
        peel(m.child).map { case (i, s) => (i, if (divide) m.scale else s) }
      case c: Cast => peel(c.child)
      case Divide(c, Literal(v, _), _) =>
        val extra = pow10Scale(v)
        if (extra < 0) None
        else peel(c).map { case (i, s) => (i, if (divide) s + extra else s) }
      case _ => None
    }
    val cols = agg.resultExpressions.map(peel)
    if (cols.exists(_.isEmpty)) {
      Left(agg.resultExpressions.map(_.prettyName).distinct.mkString(","))
    } else if (cols.flatten.length != agg.output.length) {
      Left("width")
    } else {
      Right(cols.flatten)
    }
  }

  private def identityLayout(cols: Seq[(Int, Int)]): Boolean = {
    cols.zipWithIndex.forall { case ((i, d), n) => i == n && d == 0 }
  }

  private def attrId(e: Expression): Option[ExprId] = e match {
    case a: Attribute => Some(a.exprId)
    case a: Alias => Some(a.exprId)
    case _ => None
  }

  /** 100, 100.0 and 10^scale literals. -1 when `v` is not a power of ten. */
  private def pow10Scale(v: Any): Int = {
    val d = v match {
      case n: java.lang.Double => n.doubleValue()
      case n: java.lang.Float => n.doubleValue()
      case n: java.lang.Long => n.doubleValue()
      case n: java.lang.Integer => n.doubleValue()
      case n: Decimal => n.toDouble
      case _ => return -1
    }
    if (d <= 0.0 || d.isNaN || d.isInfinity) {
      return -1
    }
    val r = math.round(math.log10(d)).toInt
    val back = math.pow(10.0, r.toDouble)
    if (math.abs(back - d) <= 1e-6 * d) r else -1
  }

  private def flattenFilters(e: Expression): Seq[Expression] = e match {
    case And(l, r) => flattenFilters(l) ++ flattenFilters(r)
    case IsNotNull(_) => Seq.empty
    case other => Seq(other)
  }

  private def parseFilters(exprs: Seq[Expression]): Either[CutSkip, Seq[FilterPred]] = {
    val parsed = exprs.map(e => e -> parseOneCmp(e))
    val bad = parsed.collect { case (e, None) => e.prettyName }
    if (bad.nonEmpty) {
      Left(CutSkip("residual-filter:" + bad.mkString(","), "filter"))
    } else {
      Right(parsed.flatMap(_._2).distinct)
    }
  }

  private def parseOneCmp(e: Expression): Option[FilterPred] = e match {
    case GreaterThan(left, Literal(v, _)) =>
      leafName(left).flatMap(n => numPred(n, v, NativePlan.FILTER_GT))
    case GreaterThanOrEqual(left, Literal(v, _)) =>
      leafName(left).flatMap(n => numPred(n, v, NativePlan.FILTER_GTE))
    case LessThan(left, Literal(v, _)) =>
      leafName(left).flatMap(n => numPred(n, v, NativePlan.FILTER_LT))
    case LessThanOrEqual(left, Literal(v, _)) =>
      leafName(left).flatMap(n => numPred(n, v, NativePlan.FILTER_LTE))
    case EqualTo(left, Literal(v, _)) =>
      leafName(left).flatMap(n => eqPred(n, v, NativePlan.FILTER_EQ))
    case EqualTo(left, right) =>
      for {
        a <- leafName(left)
        b <- leafName(right)
      } yield FilterPred(a, 0L, NativePlan.FILTER_EQ, rightCol = b)
    case EqualNullSafe(left, Literal(v, _)) =>
      leafName(left).flatMap(n => eqPred(n, v, NativePlan.FILTER_EQ))
    case Not(EqualTo(left, Literal(v, _))) =>
      leafName(left).flatMap(n => eqPred(n, v, NativePlan.FILTER_NE))
    case _ => None
  }

  private def numPred(name: String, v: Any, op: Int): Option[FilterPred] = v match {
    case d: java.lang.Double => Some(FilterPred(name, 0L, op, dvalue = d.doubleValue()))
    case f: java.lang.Float => Some(FilterPred(name, 0L, op, dvalue = f.doubleValue()))
    case dec: Decimal => Some(FilterPred(name, 0L, op, dvalue = dec.toDouble))
    case other => toLong(other).map(FilterPred(name, _, op))
  }

  private def eqPred(name: String, v: Any, op: Int): Option[FilterPred] = v match {
    case s: UTF8String => Some(FilterPred(name, 0L, op, strValue = s.toString))
    case s: String => Some(FilterPred(name, 0L, op, strValue = s))
    case other => numPred(name, other, op)
  }

  private def hasJoin(plan: SparkPlan): Boolean = unwrap(plan) match {
    case _: BroadcastHashJoinExec => true
    case _: SortMergeJoinExec => true
    case p => p.children.exists(hasJoin)
  }

  private def hasWindow(plan: SparkPlan): Boolean = unwrap(plan) match {
    case _: WindowExec => true
    case p => p.children.exists(hasWindow)
  }

  private def exprScale(e: Expression): Int = e match {
    case u: UnscaledValue => exprScale(u.child)
    case c: Cast => exprScale(c.child)
    case Alias(c, _) => exprScale(c)
    case _ => e.dataType match {
      case d: DecimalType => d.scale
      case _ => 0
    }
  }

  private def sumScale(agg: BaseAggregateExec): Int = {
    agg.aggregateExpressions.head.aggregateFunction match {
      case s: Sum => exprScale(s.child)
      case _ => 0
    }
  }

  private def sumName(agg: BaseAggregateExec): Option[String] = {
    agg.aggregateExpressions.head.aggregateFunction match {
      case s: Sum => leafName(s.child)
      case _ => None
    }
  }

  private def leafName(e: Expression): Option[String] = e match {
    case a: Attribute => Some(a.name)
    case Alias(c, _) => leafName(c)
    case c: Cast => leafName(c.child)
    case u: UnscaledValue => leafName(u.child)
    case _ => None
  }

  private def toLong(v: Any): Option[Long] = v match {
    case i: java.lang.Integer => Some(i.longValue())
    case l: java.lang.Long => Some(l.longValue())
    case i: Int => Some(i.toLong)
    case l: Long => Some(l)
    case d: Decimal => Some(d.toLong)
    case s: UTF8String =>
      try {
        Some(s.toString.toLong)
      } catch {
        case _: NumberFormatException => None
      }
    case _ => None
  }
}
