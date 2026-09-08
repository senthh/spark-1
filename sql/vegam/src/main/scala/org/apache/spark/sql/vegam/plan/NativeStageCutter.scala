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
  ColumnarToRowExec, ExpandExec, FileSourceScanExec, FilterExec, InputAdapter, ProjectExec,
  SortExec, SparkPlan, UnionExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.exchange.Exchange
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.aggregate.{BaseAggregateExec, HashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, FileScan}
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.types.{Decimal, DecimalType, LongType, StringType}
import org.apache.spark.sql.vegam.VegamPaths
import org.apache.spark.unsafe.types.UTF8String

import scala.util.{Either, Left, Right}

/**
 * Lowers one Spark stage to [[NativePlan]] or rejects it. Never walks
 * [[QueryStageExec]]: a later AQE Final must not re-read the original files.
 */
object NativeStageCutter {

  sealed trait CutResult
  case class CutOk(plan: NativePlan) extends CutResult
  case class CutSkip(why: String, detail: String) extends CutResult

  def cut(plan: SparkPlan): CutResult = unwrap(plan) match {
    case agg: HashAggregateExec => cutAgg(agg)
    case agg: SortAggregateExec => cutAgg(agg)
    case w: WindowExec => cutWindow(w)
    case other => CutSkip("unsupported-root", other.nodeName)
  }

  private def cutAgg(agg: BaseAggregateExec): CutResult = {
    if (isCountStar(agg) && !hasJoin(agg) && !hasWindow(agg)) {
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
          CutOk(CountStar(pipe.probe.paths.map(VegamPaths.clean)))
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
    // Partial Average is two buffers (sum, count). Do not emit a final avg.
    if (agg.aggregateExpressions.exists(e =>
        e.mode == Partial && e.aggregateFunction.isInstanceOf[Average])) {
      return CutSkip("partial-avg", "avg")
    }
    lowerPipeline(agg.child) match {
      case Left(skip) => skip
      case Right(pipe) =>
        val complete = agg.aggregateExpressions.headOption.exists(_.mode == Complete)
        if (isSimpleGroupSum(agg, pipe)) {
          CutOk(toHashAgg(agg, pipe, complete))
        } else {
            CutOk(StagePlan(
              probe = pipe.probe,
              builds = pipe.builds,
              probeFilters = pipe.probeFilters ++ pipe.scanFilters,
              groups = parsed.groups,
              groupTypes = parsed.groupTypes,
              aggs = parsed.aggs,
              window = pipe.window,
              complete = complete,
              expand = pipe.expand))
        }
    }
  }

  private def cutWindow(w: WindowExec): CutResult = {
    parseWindow(w) match {
      case Left(skip) => skip
      case Right(spec) =>
        lowerPipeline(w.child) match {
          case Left(skip) => skip
          case Right(pipe) if pipe.window.isDefined =>
            CutSkip("nested-window", "window")
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
              expand = pipe.expand))
        }
    }
  }

  private case class Pipe(
      probe: ScanSpec,
      builds: Seq[BuildJoin],
      probeFilters: Seq[FilterPred],
      scanFilters: Seq[FilterPred],
      window: Option[WinSpec],
      expand: Option[ExpandSpec] = None)

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
        parseFilters(flattenFilters(f.condition)) match {
          case Left(skip) => Left(skip)
          case Right(preds) =>
            lowerPipeline(f.child).map(p => p.copy(probeFilters = p.probeFilters ++ preds))
        }
      case p: ProjectExec =>
        if (p.projectList.forall(e => leafName(e).isDefined || e.isInstanceOf[Attribute])) {
          lowerPipeline(p.child)
        } else {
          Left(CutSkip("project-expr", p.projectList.map(_.prettyName).mkString(",")))
        }
      case s: FileSourceScanExec =>
        scanToPipe(v1Scan(s))
      case b: BatchScanExec =>
        v2Scan(b) match {
          case None => Left(CutSkip("v2-scan", b.scan.getClass.getName))
          case Some(info) => scanToPipe(info)
        }
      case a: BaseAggregateExec =>
        Left(CutSkip("nested-agg", a.nodeName))
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
            window = None))
        }
    }
  }

  private def lowerJoin(j: BroadcastHashJoinExec): Either[CutSkip, Pipe] = {
    if (j.condition.isDefined) {
      return Left(CutSkip("join-cond", j.joinType.sql))
    }
    joinKind(j.joinType) match {
      case None => Left(CutSkip("join-type", j.joinType.sql))
      case Some(kind) =>
        val (probeP, buildP, pKeys, bKeys) = j.buildSide match {
          case BuildRight => (j.left, j.right, j.leftKeys, j.rightKeys)
          case BuildLeft => (j.right, j.left, j.rightKeys, j.leftKeys)
        }
        val pk = pKeys.flatMap(leafName)
        val bk = bKeys.flatMap(leafName)
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
        val pk = j.leftKeys.flatMap(leafName)
        val bk = j.rightKeys.flatMap(leafName)
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
            filters = right.probeFilters ++ right.scanFilters)
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
      case CutOk(_) =>
        parseFilters(scan.dataFilters.flatMap(flattenFilters)) match {
          case Left(skip) => Left(skip)
          case Right(preds) =>
            Right(Pipe(
              probe = ScanSpec(scan.refs, scan.columns),
              builds = Nil,
              probeFilters = Nil,
              scanFilters = preds,
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
      detail: String)

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
      detail = index.toString)
  }

  private def v2Scan(scan: BatchScanExec): Option[ScanInfo] = scan.scan match {
    case p: ParquetScan =>
      Some(fromFileIndex(p.fileIndex, p.dataFilters, parquet = true, scan.output.map(_.name),
        "parquet-v2"))
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
      agg.groupingExpressions.length == 1 &&
      agg.aggregateExpressions.length == 1 &&
      agg.aggregateExpressions.head.aggregateFunction.isInstanceOf[Sum]
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
    e.aggregateFunction match {
      case _: Count if e.aggregateFunction.children.isEmpty ||
          e.aggregateFunction.prettyName == "count" =>
        val star = e.aggregateFunction.children.isEmpty ||
          e.aggregateFunction.children.exists(_.isInstanceOf[Literal])
        if (star) {
          Some(AggCall(NativePlan.AGG_COUNT_STAR, "", 0, LongType))
        } else {
          leafName(e.aggregateFunction.children.head)
            .map(n => AggCall(NativePlan.AGG_COUNT, n, 0, LongType))
        }
      case c: Count =>
        val child = c.children.headOption
        if (child.exists(_.isInstanceOf[Literal])) {
          Some(AggCall(NativePlan.AGG_COUNT_STAR, "", 0, LongType))
        } else {
          child.flatMap(leafName).map(n => AggCall(NativePlan.AGG_COUNT, n, 0, LongType))
        }
      case s: Sum =>
        leafName(s.child).map(n => AggCall(NativePlan.AGG_SUM, n, exprScale(s.child), dt))
      case m: Min =>
        leafName(m.child).map(n => AggCall(NativePlan.AGG_MIN, n, exprScale(m.child), dt))
      case m: Max =>
        leafName(m.child).map(n => AggCall(NativePlan.AGG_MAX, n, exprScale(m.child), dt))
      case a: Average =>
        leafName(a.child).map(n => AggCall(NativePlan.AGG_AVG, n, exprScale(a.child), dt))
      case _ => None
    }
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
      case WindowExpression(fn, _) =>
        fn match {
          case _: RowNumber => Some(WindowCall(NativePlan.WIN_ROW_NUMBER, "", alias))
          case _: Rank => Some(WindowCall(NativePlan.WIN_RANK, "", alias))
          case _: DenseRank => Some(WindowCall(NativePlan.WIN_DENSE_RANK, "", alias))
          case a: AggregateExpression =>
            a.aggregateFunction match {
              case s: Sum =>
                leafName(s.child).map(n => WindowCall(NativePlan.WIN_SUM, n, alias))
              case _ => None
            }
          case _ => None
        }
      case _ => None
    }
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
