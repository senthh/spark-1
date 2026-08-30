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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types._

/**
 * Compact S-expression IR consumed by the Native SQL C++ engine.
 *
 * Supported subtree (primitive int / long / double / boolean):
 *   scan -> Filter col cmp literal -> Project col+/-col -> hash agg / hash join / sort
 *
 * File scans (LogicalRelation + HadoopFsRelation) are never compiled. The hybrid
 * engine must let FileSourceStrategy plan the scan; later a partition-level native
 * op will wrap it. Swallowing FileScan as NativeSqlExec (LeafExecNode) would pull
 * data to the driver.
 */
object NativeSqlPlan {

  val supportedAtomic: Set[DataType] =
    Set(IntegerType, LongType, DoubleType, BooleanType, ByteType, ShortType, FloatType)

  def isSupportedType(dt: DataType): Boolean = supportedAtomic.contains(dt)

  /**
   * True when `plan` is a v1 file-scan leaf. Used by tests and hybrid dispatch.
   *
   * The hybrid engine must let FileSourceStrategy plan the scan; later a
   * partition-level native op will wrap it. Swallowing FileScan as
   * NativeSqlExec (LeafExecNode) would pull data to the driver.
   */
  def isFileScanLeaf(plan: LogicalPlan): Boolean = plan match {
    case LogicalRelation(_: HadoopFsRelation, _, _, _, _) => true
    case _ => false
  }

  def compile(plan: LogicalPlan): Option[Compiled] = compile0(plan)

  case class Compiled(ir: String, leaves: Seq[LeafData], output: Seq[Attribute])

  sealed trait LeafData
  case class LocalLeaf(output: Seq[Attribute], rows: Seq[org.apache.spark.sql.catalyst.InternalRow])
    extends LeafData
  case class RangeLeaf(start: Long, end: Long, step: Long) extends LeafData

  private def compile0(plan: LogicalPlan): Option[Compiled] = plan match {
    case l: LocalRelation if l.output.forall(a => isSupportedType(a.dataType)) =>
      Some(Compiled(s"(scan 0)", Seq(LocalLeaf(l.output, l.data)), l.output))

    case r: Range if r.step != 0 && r.numElements <= Int.MaxValue =>
      Some(Compiled(
        s"(range ${r.start} ${r.end} ${r.step})",
        Seq(RangeLeaf(r.start, r.end, r.step)),
        r.output))

    // Do not load Parquet (or any HadoopFsRelation) in C++ / on the driver.
    case p if isFileScanLeaf(p) => None

    case Filter(cond, child) =>
      for {
        c <- compile0(child)
        pred <- compilePred(cond, c.output)
      } yield c.copy(ir = s"(filter $pred ${c.ir})", output = child.output)

    case Project(projectList, child) =>
      compile0(child).flatMap { c =>
        val exprs = projectList.map(compileNamed(_, c.output))
        if (exprs.forall(_.isDefined)) {
          Some(c.copy(
            ir = s"(project (list ${exprs.map(_.get).mkString(" ")}) ${c.ir})",
            output = projectList.map(_.toAttribute)))
        } else None
      }

    case Sort(order, _, child, _)
        if order.nonEmpty &&
          order.forall(o => o.direction == Ascending && isSupportedType(o.dataType)) =>
      compile0(child).flatMap { c =>
        val keys = order.map(o => compileExpr(o.child, c.output))
        if (keys.forall(_.isDefined)) {
          Some(c.copy(
            ir = s"(sort (list ${keys.map(_.get).mkString(" ")}) ${c.ir})",
            output = c.output))
        } else None
      }

    case Aggregate(grouping, agg, child, _) if grouping.size <= 2 =>
      compile0(child).flatMap { c =>
        val keys = grouping.map(compileExpr(_, c.output))
        val aggs = agg.map(compileNamed(_, c.output))
        if (keys.forall(_.isDefined) && aggs.forall(_.isDefined) &&
            grouping.forall(e => isSupportedType(e.dataType))) {
          Some(c.copy(
            ir = s"(hashagg (list ${keys.map(_.get).mkString(" ")}) (list ${aggs.map(_.get).mkString(" ")}) ${c.ir})",
            output = agg.map(_.toAttribute)))
        } else None
      }

    case Join(left, right, Inner, Some(cond), _) =>
      for {
        l <- compile0(left)
        r <- compile0(right)
        (lk, rk) <- compileJoinKeys(cond, l.output, r.output)
        // shift right-side scan indices
        rIr = shiftScans(r.ir, l.leaves.size)
      } yield Compiled(
        s"(hashjoin $lk $rk ${l.ir} $rIr)",
        l.leaves ++ r.leaves,
        left.output ++ right.output)

    case _ => None
  }

  private def shiftScans(ir: String, delta: Int): String = {
    if (delta == 0) ir
    else """\(scan (\d+)\)""".r.replaceAllIn(ir, m => s"(scan ${m.group(1).toInt + delta})")
  }

  private def compilePred(e: Expression, schema: Seq[Attribute]): Option[String] = e match {
    case And(l, r) =>
      for (a <- compilePred(l, schema); b <- compilePred(r, schema)) yield s"(and $a $b)"
    case EqualTo(l, r) => cmp("eq", l, r, schema)
    case EqualNullSafe(l, r) => cmp("eq", l, r, schema)
    case GreaterThan(l, r) => cmp("gt", l, r, schema)
    case GreaterThanOrEqual(l, r) => cmp("ge", l, r, schema)
    case LessThan(l, r) => cmp("lt", l, r, schema)
    case LessThanOrEqual(l, r) => cmp("le", l, r, schema)
    case Not(EqualTo(l, r)) => cmp("ne", l, r, schema)
    case _ => None
  }

  private def cmp(op: String, l: Expression, r: Expression, schema: Seq[Attribute]): Option[String] = {
    (compileExpr(l, schema), compileExpr(r, schema)) match {
      case (Some(a), Some(b)) => Some(s"($op $a $b)")
      case _ => None
    }
  }

  private def compileNamed(e: NamedExpression, schema: Seq[Attribute]): Option[String] = e match {
    case Alias(child, _) => compileExpr(child, schema)
    case other => compileExpr(other, schema)
  }

  private def compileExpr(e: Expression, schema: Seq[Attribute]): Option[String] = e match {
    case a: Attribute =>
      val i = schema.indexWhere(_.exprId == a.exprId)
      if (i >= 0) Some(s"c$i") else None
    case Literal(null, _) => None
    case Literal(v, IntegerType) => Some(s"${v}i32")
    case Literal(v, LongType) => Some(s"${v}i64")
    case Literal(v, DoubleType) => Some(s"${v}f64")
    case Literal(v, FloatType) => Some(s"${v.toString.toDouble}f64")
    case Literal(v, BooleanType) => Some(if (v == true) "true" else "false")
    case Literal(v, ByteType) => Some(s"${v}i32")
    case Literal(v, ShortType) => Some(s"${v}i32")
    case Add(l, r, _) => bin("add", l, r, schema)
    case Subtract(l, r, _) => bin("sub", l, r, schema)
    case Multiply(l, r, _) => bin("mul", l, r, schema)
    case Divide(l, r, _) => bin("div", l, r, schema)
    case UnaryMinus(c, _) => compileExpr(c, schema).map(x => s"(neg $x)")
    case Cast(c, dt, _, _) if isSupportedType(dt) => compileExpr(c, schema)
    case AggregateExpression(af, _, _, _, _) => compileAgg(af, schema)
    case af: AggregateFunction => compileAgg(af, schema)
    case _ => None
  }

  private def compileAgg(af: AggregateFunction, schema: Seq[Attribute]): Option[String] = af match {
    case Sum(c, _) => compileExpr(c, schema).map(x => s"(sum $x)")
    case Count(Nil) => Some("(count)")
    case Count(Seq(Literal(1, _))) => Some("(count)")
    case Count(Seq(c)) => compileExpr(c, schema).map(x => s"(count $x)")
    case Min(c) => compileExpr(c, schema).map(x => s"(min $x)")
    case Max(c) => compileExpr(c, schema).map(x => s"(max $x)")
    case Average(c, _) => compileExpr(c, schema).map(x => s"(avg $x)")
    case _ => None
  }

  private def bin(op: String, l: Expression, r: Expression, schema: Seq[Attribute]): Option[String] =
    for (a <- compileExpr(l, schema); b <- compileExpr(r, schema)) yield s"($op $a $b)"

  private def compileJoinKeys(
      cond: Expression,
      left: Seq[Attribute],
      right: Seq[Attribute]): Option[(String, String)] = cond match {
    case EqualTo(l, r) =>
      (compileExpr(l, left), compileExpr(r, right)) match {
        case (Some(a), Some(b)) => Some((a, b))
        case _ =>
          (compileExpr(l, right), compileExpr(r, left)) match {
            case (Some(a), Some(b)) => Some((b, a))
            case _ => None
          }
      }
    case And(l, r) =>
      // single-key engine: only accept one equality
      compileJoinKeys(l, left, right).orElse(compileJoinKeys(r, left, right))
    case _ => None
  }
}
