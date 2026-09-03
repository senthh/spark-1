package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.QueryStageExec
import org.apache.spark.sql.execution.{
  ColumnarRule, FileSourceScanExec, FilterExec, InputAdapter, ProjectExec, SparkPlan,
  WholeStageCodegenExec}
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.morsel.{
  MorselCountExec, MorselHashAggExec, MorselScanExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String

/**
 * Rewrites COUNT(*) to a footer read, and group-by SUM to a fused C++ pipeline,
 * when spark.sql.morsel.enabled is true.
 */
class MorselColumnarRule extends ColumnarRule {

  override def preColumnarTransitions: Rule[SparkPlan] = {
    new Rule[SparkPlan] {
      override def apply(plan: SparkPlan): SparkPlan = {
        if (!SQLConf.get.getConfString("spark.sql.morsel.enabled", "false").toBoolean) {
          return plan
        }

        plan.transformDown {
          case agg: HashAggregateExec if isCountStar(agg) =>
            findScan(agg).flatMap(scanPath) match {
              case Some(path) => MorselCountExec(path, agg.output)
              case None => agg
            }

          case agg: HashAggregateExec if isGroupSum(agg) =>
            (findScan(agg).flatMap(scanPath), groupName(agg), sumName(agg)) match {
              case (Some(path), Some(g), Some(s)) =>
                val filt = extractGt(agg)
                MorselHashAggExec(
                  filePath = path,
                  groupCol = g,
                  sumCol = s,
                  filterCol = filt.map(_._1),
                  filterValue = filt.map(_._2).getOrElse(0L),
                  output = agg.output)
              case _ => agg
            }

          case scan: FileSourceScanExec =>
            val paths = scan.relation.location.rootPaths
            if (paths.isEmpty) {
              scan
            } else {
              MorselScanExec(
                filePath = paths.head.toString,
                output = scan.output,
                filterCol = -1,
                filterValue = 0)
            }
        }
      }
    }
  }

  private def unwrap(plan: SparkPlan): SparkPlan = plan match {
    case w: WholeStageCodegenExec => unwrap(w.child)
    case i: InputAdapter => unwrap(i.child)
    case p: ProjectExec => unwrap(p.child)
    case other => other
  }

  private def findScan(plan: SparkPlan): Option[FileSourceScanExec] = {
    unwrap(plan) match {
      case s: FileSourceScanExec => Some(s)
      case f: FilterExec => findScan(f.child)
      case a: HashAggregateExec => findScan(a.child)
      case q: QueryStageExec => findScan(q.plan)
      case p => p.children.iterator.map(findScan).collectFirst { case Some(s) => s }
    }
  }

  private def scanPath(scan: FileSourceScanExec): Option[String] = {
    scan.relation.location.rootPaths.headOption.map(_.toString)
  }

  private def isSupportedMode(mode: AggregateMode): Boolean = {
    mode == Final || mode == Complete || mode == Partial
  }

  private def isCountStar(agg: HashAggregateExec): Boolean = {
    agg.groupingExpressions.isEmpty &&
      agg.aggregateExpressions.length == 1 && {
        val e = agg.aggregateExpressions.head
        e.aggregateFunction.isInstanceOf[Count] && isSupportedMode(e.mode)
      } && findScan(agg).isDefined && !hasResidualFilter(agg)
  }

  private def isGroupSum(agg: HashAggregateExec): Boolean = {
    agg.groupingExpressions.length == 1 &&
      agg.aggregateExpressions.length == 1 && {
        val e = agg.aggregateExpressions.head
        e.aggregateFunction.isInstanceOf[Sum] && isSupportedMode(e.mode)
      } && findScan(agg).isDefined
  }

  private def hasResidualFilter(plan: SparkPlan): Boolean = {
    unwrap(plan) match {
      case _: FilterExec => true
      case s: FileSourceScanExec => s.dataFilters.nonEmpty
      case p => p.children.exists(hasResidualFilter)
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

  private def extractGt(plan: SparkPlan): Option[(String, Long)] = {
    def fromExpr(e: Expression): Option[(String, Long)] = e match {
      case GreaterThan(left, Literal(v, _)) =>
        leafName(left).flatMap(n => toLong(v).map((n, _)))
      case GreaterThanOrEqual(left, Literal(v, _)) =>
        leafName(left).flatMap(n => toLong(v).map((n, _)))
      case And(l, r) => fromExpr(l).orElse(fromExpr(r))
      case _ => None
    }
    unwrap(plan) match {
      case f: FilterExec => fromExpr(f.condition).orElse(extractGt(f.child))
      case s: FileSourceScanExec =>
        s.dataFilters.iterator.map(fromExpr).collectFirst { case Some(x) => x }
      case a: HashAggregateExec => extractGt(a.child)
      case p => p.children.iterator.map(extractGt).collectFirst { case Some(x) => x }
    }
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
