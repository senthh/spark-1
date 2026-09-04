package org.apache.spark.sql.execution.columnar

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{
  ColumnarRule, FileSourceScanExec, FilterExec, InputAdapter, ProjectExec, SparkPlan,
  WholeStageCodegenExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.morsel.{
  MorselCountExec, MorselHashAggExec, MorselPaths, MorselScanExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

/**
 * Rewrites COUNT(*) to a footer read, and group-by SUM to a fused C++ pipeline,
 * when spark.sql.morsel.enabled is true.
 */
class MorselColumnarRule extends ColumnarRule with Logging {

  override def preColumnarTransitions: Rule[SparkPlan] = {
    new Rule[SparkPlan] {
      override def apply(plan: SparkPlan): SparkPlan = {
        if (!SQLConf.get.getConfString("spark.sql.morsel.enabled", "false").toBoolean) {
          return plan
        }

        plan.transformDown {
          case agg: HashAggregateExec if isCountStarShape(agg) =>
            rewriteCount(agg)

          case agg: HashAggregateExec if isGroupSumShape(agg) =>
            rewriteGroupSum(agg)

          // MorselScanExec reports a row count and no column values, so it can
          // only replace a scan that feeds nothing but row counts downstream.
          case scan: FileSourceScanExec if servesRowCountsOnly(scan) =>
            posixFiles(scan) match {
              case Some(files) =>
                MorselScanExec(filePaths = files, output = scan.output)
              case None =>
                scan
            }
        }
      }
    }
  }

  private def skip(why: String, detail: String): Unit = {
    val msg = s"morsel: skip $why $detail"
    logInfo(msg)
    System.err.println(msg)
  }

  private def servesRowCountsOnly(scan: FileSourceScanExec): Boolean = {
    scan.output.isEmpty &&
      scan.requiredSchema.isEmpty &&
      scan.dataFilters.isEmpty &&
      scan.partitionFilters.isEmpty
  }

  private def unwrap(plan: SparkPlan): SparkPlan = plan match {
    case w: WholeStageCodegenExec => unwrap(w.child)
    case i: InputAdapter => unwrap(i.child)
    case p: ProjectExec => unwrap(p.child)
    case other => other
  }

  // Do not walk QueryStageExec: a Final in a later AQE stage would re-read
  // the original files and drop the shuffle.
  private def findScan(plan: SparkPlan): Option[FileSourceScanExec] = {
    unwrap(plan) match {
      case s: FileSourceScanExec => Some(s)
      case f: FilterExec => findScan(f.child)
      case a: HashAggregateExec => findScan(a.child)
      case p => p.children.iterator.map(findScan).collectFirst { case Some(s) => s }
    }
  }

  private def posixFiles(scan: FileSourceScanExec): Option[Seq[String]] = {
    if (scan.relation.partitionSchema.nonEmpty) {
      skip("partitioned", scan.relation.location.toString)
      return None
    }
    val files = scan.relation.location.inputFiles
    if (files.isEmpty) {
      skip("no-files", scan.relation.location.toString)
      None
    } else if (!files.forall(MorselPaths.isPosix)) {
      skip("non-posix", files.take(3).mkString(","))
      None
    } else {
      Some(files.toSeq)
    }
  }

  private def isCountStarShape(agg: HashAggregateExec): Boolean = {
    agg.groupingExpressions.isEmpty &&
      agg.aggregateExpressions.length == 1 && {
        val e = agg.aggregateExpressions.head
        e.aggregateFunction.isInstanceOf[Count] &&
          (e.mode == Final || e.mode == Complete || e.mode == Partial)
      }
  }

  // Rewrite Partial or Complete only. Spark's Final merges per-file sums.
  private def isGroupSumShape(agg: HashAggregateExec): Boolean = {
    agg.groupingExpressions.length == 1 &&
      agg.aggregateExpressions.length == 1 && {
        val e = agg.aggregateExpressions.head
        e.aggregateFunction.isInstanceOf[Sum] &&
          (e.mode == Partial || e.mode == Complete)
      }
  }

  private def rewriteCount(agg: HashAggregateExec): SparkPlan = {
    findScan(agg) match {
      case None =>
        skip("no-scan", "count")
        agg
      case Some(scan) =>
        if (hasUnsupportedFilter(agg)) {
          skip("residual-filter", "count")
          agg
        } else {
          posixFiles(scan) match {
            case Some(files) => MorselCountExec(filePaths = files, output = agg.output)
            case None => agg
          }
        }
    }
  }

  private def rewriteGroupSum(agg: HashAggregateExec): SparkPlan = {
    val scanOpt = findScan(agg)
    if (scanOpt.isEmpty) {
      skip("no-scan", "group-sum")
      return agg
    }
    val g = groupName(agg)
    val s = sumName(agg)
    if (g.isEmpty || s.isEmpty) {
      skip("unsupported-expr", s"group=${g.isDefined} sum=${s.isDefined}")
      return agg
    }
    parsePlanFilter(agg) match {
      case Left(why) =>
        skip(why, s"${g.get},${s.get}")
        agg
      case Right(filt) =>
        posixFiles(scanOpt.get) match {
          case Some(files) =>
            MorselHashAggExec(
              filePaths = files,
              groupCol = g.get,
              sumCol = s.get,
              filterCol = filt.map(_.col),
              filterValue = filt.map(_.value).getOrElse(0L),
              filterOp = filt.map(_.op).getOrElse(0),
              output = agg.output)
          case None => agg
        }
    }
  }

  private def hasUnsupportedFilter(plan: SparkPlan): Boolean = {
    filterExprs(plan).exists(parseOneCmp(_).isEmpty)
  }

  private case class BoundFilter(col: String, value: Long, op: Int)

  private def parseOneCmp(e: Expression): Option[BoundFilter] = e match {
    case GreaterThan(left, Literal(v, _)) =>
      leafName(left).flatMap(n => toLong(v).map(BoundFilter(n, _, 1)))
    case GreaterThanOrEqual(left, Literal(v, _)) =>
      leafName(left).flatMap(n => toLong(v).map(BoundFilter(n, _, 2)))
    case _ => None
  }

  private def flattenFilters(e: Expression): Seq[Expression] = e match {
    case And(l, r) => flattenFilters(l) ++ flattenFilters(r)
    case IsNotNull(_) => Seq.empty
    case other => Seq(other)
  }

  private def filterExprs(plan: SparkPlan): Seq[Expression] = {
    unwrap(plan) match {
      case f: FilterExec => flattenFilters(f.condition) ++ filterExprs(f.child)
      case s: FileSourceScanExec => s.dataFilters.flatMap(flattenFilters)
      case a: HashAggregateExec => filterExprs(a.child)
      case p => p.children.flatMap(filterExprs)
    }
  }

  // Right(None) = no filter. Left = residual / unsupported. Never take one
  // conjunct and drop the rest.
  private def parsePlanFilter(plan: SparkPlan): Either[String, Option[BoundFilter]] = {
    val parsed = filterExprs(plan).map { e =>
      parseOneCmp(e) match {
        case Some(b) => Right(b)
        case None => Left(e.prettyName)
      }
    }
    val bad = parsed.collect { case Left(n) => n }
    if (bad.nonEmpty) {
      return Left("residual-filter:" + bad.mkString(","))
    }
    val ok = parsed.collect { case Right(b) => b }.distinct
    if (ok.isEmpty) {
      Right(None)
    } else if (ok.size == 1) {
      Right(Some(ok.head))
    } else {
      Left("multi-filter")
    }
  }

  private def groupName(agg: HashAggregateExec): Option[String] = {
    leafName(agg.groupingExpressions.head)
  }

  private def sumName(agg: HashAggregateExec): Option[String] = {
    agg.aggregateExpressions.head.aggregateFunction match {
      case s: Sum => leafName(s.child)
      case _ => None
    }
  }

  private def leafName(e: Expression): Option[String] = e match {
    case a: Attribute => Some(a.name)
    case Alias(c, _) => leafName(c)
    case c: Cast => leafName(c.child)
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

object MorselColumnarRule {
  def apply(): MorselColumnarRule = new MorselColumnarRule()
}
