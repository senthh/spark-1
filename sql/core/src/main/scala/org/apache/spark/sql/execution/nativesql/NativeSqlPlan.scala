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
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Compact S-expression IR consumed by the Native SQL C++ engine.
 *
 * File star-schema: Filter/Project stay on FileLeaf (Parquet pushdown).
 * Join / hashagg / semi / union / CASE run in C++. Strings are i64 hashes;
 * decimals are unscaled i64. File-backed hashagg is partial; NativeSqlExec merges.
 */
object NativeSqlPlan {

  val supportedAtomic: Set[DataType] =
    Set(IntegerType, LongType, DoubleType, BooleanType, ByteType, ShortType, FloatType, DateType)

  def isSupportedType(dt: DataType): Boolean = dt match {
    case DateType => true
    case d: DecimalType if d.precision <= 18 => true
    case _ => supportedAtomic.contains(dt)
  }

  /** Columns we can encode into a C++ batch (hash strings, unscaled decimals). */
  def isFilePayloadType(dt: DataType): Boolean = dt match {
    case StringType => true
    case d: DecimalType if d.precision <= 18 => true
    case _: AtomicType => true
    case _ => false
  }

  sealed trait NativeAggKind
  object NativeAggKind {
    case object Pass extends NativeAggKind
    case object Sum extends NativeAggKind
    case object Count extends NativeAggKind
    case object Min extends NativeAggKind
    case object Max extends NativeAggKind
    case object AvgSum extends NativeAggKind
    case object AvgCnt extends NativeAggKind
  }

  /**
   * True when `plan` is a v1 file-scan leaf. Used by tests and hybrid dispatch.
   */
  def isFileScanLeaf(plan: LogicalPlan): Boolean = unwrapAlias(plan) match {
    case LogicalRelation(_: HadoopFsRelation, _, _, _, _) => true
    case _ => false
  }

  @scala.annotation.tailrec
  private def unwrapAlias(plan: LogicalPlan): LogicalPlan = plan match {
    case SubqueryAlias(_, child) => unwrapAlias(child)
    case other => other
  }

  def compile(plan: LogicalPlan): Option[Compiled] = compile0(plan)

  case class Compiled(
      ir: String,
      leaves: Seq[LeafData],
      output: Seq[Attribute],
      joinKeyCols: Option[(Int, Int)] = None,
      joinKeyNames: Option[(String, String)] = None,
      leafOutputs: Seq[Seq[Attribute]] = Nil,
      aggKinds: Seq[NativeAggKind] = Nil) {
    def hasFileLeaf: Boolean = leaves.exists(_.isInstanceOf[FileLeaf])
    def fileRels: Seq[LogicalPlan] = leaves.collect { case FileLeaf(p) => p }
    def isPassthroughScan: Boolean = ir.matches("""\(scan \d+\)""") || ir.startsWith("(range ")
    def isHeavy: Boolean =
      ir.contains("hashjoin") || ir.contains("hashagg") || ir.contains("hashsemi") ||
        ir.contains("(union ") || fileRels.size >= 2
    def withLeafOutputs: Compiled = {
      if (leafOutputs.nonEmpty) this
      else copy(leafOutputs = leaves.collect { case FileLeaf(p) => p.output })
    }
  }

  sealed trait LeafData
  case class LocalLeaf(output: Seq[Attribute], rows: Seq[org.apache.spark.sql.catalyst.InternalRow])
    extends LeafData
  case class RangeLeaf(start: Long, end: Long, step: Long) extends LeafData
  /** File scan planned by FileSourceStrategy; executed per partition, not on the driver. */
  case class FileLeaf(@transient plan: LogicalPlan) extends LeafData

  private def compile0(plan: LogicalPlan): Option[Compiled] = plan match {
    case l: LocalRelation if l.output.forall(a => isSupportedType(a.dataType)) =>
      Some(Compiled(s"(scan 0)", Seq(LocalLeaf(l.output, l.data)), l.output))

    case r: Range if r.step != 0 && r.numElements <= Int.MaxValue =>
      Some(Compiled(
        s"(range ${r.start} ${r.end} ${r.step})",
        Seq(RangeLeaf(r.start, r.end, r.step)),
        r.output))

    case p if isFileScanLeaf(p) && p.output.forall(a => isFilePayloadType(a.dataType)) =>
      Some(Compiled(s"(scan 0)", Seq(FileLeaf(unwrapAlias(p))), p.output))

    case s: SubqueryAlias => compile0(s.child)

    case Filter(cond, child) =>
      compile0(child).flatMap { c0 =>
        val c = c0.withLeafOutputs
        val parts = splitAnd(cond)
        val (pushed, residual) = parts.partition(p => predOnOneFileLeaf(p, c))
        val newLeaves = attachPredsToLeaves(c, pushed)
        val ignorable = residual.forall(isIgnorablePred)
        val residualCompiled = residual.flatMap(p => compilePred(p, c.output)).filter(_ != "true")
        if (!ignorable && residualCompiled.size != residual.count(p => !isIgnorablePred(p))) {
          None
        } else {
          val leafPreds = pushed.flatMap(p => compilePred(p, c.output)).filter(_ != "true")
          val preds = residualCompiled ++ leafPreds
          if (preds.isEmpty) {
            Some(c.copy(leaves = newLeaves))
          } else {
            val pred = preds.reduce((a, b) => s"(and $a $b)")
            Some(c.copy(ir = s"(filter $pred ${c.ir})", leaves = newLeaves, output = c.output))
          }
        }
      }

    case Project(projectList, child) =>
      compile0(child).flatMap { c =>
        val exprs = projectList.map(compileNamed(_, c.output))
        val outTypesOk = projectList.forall(e =>
          isSupportedType(e.dataType) || e.dataType == StringType ||
            e.dataType.isInstanceOf[DecimalType])
        if (exprs.forall(_.isDefined) && (!c.hasFileLeaf || outTypesOk)) {
          Some(c.copy(
            ir = s"(project (list ${exprs.map(_.get).mkString(" ")}) ${c.ir})",
            output = projectList.map(_.toAttribute)))
        } else {
          None
        }
      }

    case Sort(order, _, child, _)
        if order.nonEmpty &&
          order.forall(o => o.direction == Ascending &&
            (isSupportedType(o.dataType) || o.dataType == StringType)) =>
      compile0(child).flatMap { c =>
        if (c.hasFileLeaf) {
          // Global order is Spark's job (TakeOrdered / SortExec on small agg output).
          None
        } else {
          val keys = order.map(o => compileExpr(o.child, c.output))
          if (keys.forall(_.isDefined)) {
            Some(c.copy(
              ir = s"(sort (list ${keys.map(_.get).mkString(" ")}) ${c.ir})",
              output = c.output))
          } else None
        }
      }

    case Aggregate(grouping, agg, child, _) if grouping.size <= 16 =>
      compile0(child).flatMap { c =>
        val keys = grouping.map(compileExpr(_, c.output))
        val groupOk = grouping.forall(e =>
          isSupportedType(e.dataType) || e.dataType == StringType ||
            e.dataType.isInstanceOf[DecimalType])
        if (c.hasFileLeaf) {
          val compiledAggs = agg.map(compileAggNamed(_, c.output))
          if (keys.forall(_.isDefined) && compiledAggs.forall(_.isDefined) && groupOk) {
            val parts = compiledAggs.map(_.get)
            val irList = parts.flatMap(_._1)
            val kinds = parts.flatMap(_._2)
            Some(c.copy(
              ir = s"(hashagg (list ${keys.map(_.get).mkString(" ")}) (list ${irList.mkString(" ")}) ${c.ir})",
              output = agg.map(_.toAttribute),
              aggKinds = kinds))
          } else None
        } else {
          val aggs = agg.map(compileNamed(_, c.output))
          if (keys.forall(_.isDefined) && aggs.forall(_.isDefined) &&
              grouping.forall(e => isSupportedType(e.dataType))) {
            Some(c.copy(
              ir = s"(hashagg (list ${keys.map(_.get).mkString(" ")}) (list ${aggs.map(_.get).mkString(" ")}) ${c.ir})",
              output = agg.map(_.toAttribute)))
          } else None
        }
      }

    case Join(left, right, Inner, Some(cond), _) =>
      for {
        l <- compile0(left)
        r <- compile0(right)
        (lk, rk) <- compileJoinKeys(cond, l.output, r.output)
        rIr = shiftScans(r.ir, l.leaves.size)
      } yield {
        Compiled(
          s"(hashjoin $lk $rk ${l.ir} $rIr)",
          l.leaves ++ r.leaves,
          left.output ++ right.output,
          joinKeyCols = Some((colIndex(lk), colIndex(rk))),
          joinKeyNames = Some((
            l.output.lift(colIndex(lk)).map(_.name).getOrElse(""),
            r.output.lift(colIndex(rk)).map(_.name).getOrElse(""))),
          leafOutputs = l.withLeafOutputs.leafOutputs ++ r.withLeafOutputs.leafOutputs)
      }

    case Join(left, right, LeftSemi, Some(cond), _) =>
      for {
        l <- compile0(left)
        r <- compile0(right)
        (lk, rk) <- compileJoinKeys(cond, l.output, r.output)
        rIr = shiftScans(r.ir, l.leaves.size)
      } yield {
        Compiled(
          s"(hashsemi $lk $rk ${l.ir} $rIr)",
          l.leaves ++ r.leaves,
          left.output,
          leafOutputs = l.withLeafOutputs.leafOutputs ++ r.withLeafOutputs.leafOutputs)
      }

    case u: Union if u.children.size >= 2 && u.children.forall(ch => compile0(ch).isDefined) =>
      val compiled = u.children.flatMap(compile0)
      if (compiled.isEmpty || compiled.exists(_.output.size != compiled.head.output.size)) {
        None
      } else {
        var offset = 0
        val ir = compiled.map { c =>
          val s = shiftScans(c.ir, offset)
          offset += c.leaves.size
          s
        }.reduce((a, b) => s"(union $a $b)")
        Some(Compiled(
          ir,
          compiled.flatMap(_.leaves),
          compiled.head.output,
          leafOutputs = compiled.flatMap(_.withLeafOutputs.leafOutputs)))
      }

    case _ => None
  }

  private def shiftScans(ir: String, delta: Int): String = {
    if (delta == 0) ir
    else """\(scan (\d+)\)""".r.replaceAllIn(ir, m => s"(scan ${m.group(1).toInt + delta})")
  }

  private def splitAnd(e: Expression): Seq[Expression] = e match {
    case And(l, r) => splitAnd(l) ++ splitAnd(r)
    case other => Seq(other)
  }

  private def compilePred(e: Expression, schema: Seq[Attribute]): Option[String] = e match {
    case And(l, r) =>
      for (a <- compilePred(l, schema); b <- compilePred(r, schema)) yield s"(and $a $b)"
    case Or(l, r) =>
      for (a <- compilePred(l, schema); b <- compilePred(r, schema)) yield s"(or $a $b)"
    // Spark injects IsNotNull next to comparisons; C++ has no null bitmap yet.
    case IsNotNull(_) => Some("true")
    case In(value, list) if list.nonEmpty && list.forall(_.foldable) =>
      val eqs = list.map(lit => cmp("eq", value, lit, schema))
      if (eqs.forall(_.isDefined)) Some(eqs.map(_.get).reduce((a, b) => s"(or $a $b)"))
      else None
    case InSet(value, hset) if hset.nonEmpty =>
      val eqs = hset.toSeq.map(v => cmp("eq", value, Literal(v, value.dataType), schema))
      if (eqs.forall(_.isDefined)) Some(eqs.map(_.get).reduce((a, b) => s"(or $a $b)"))
      else None
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
    case Literal(v, DateType) => Some(s"${v}i32")
    case Literal(v: UTF8String, StringType) => Some(s"${hash64(v)}i64")
    case Literal(v: String, StringType) => Some(s"${hash64(UTF8String.fromString(v))}i64")
    case Literal(v: Decimal, _: DecimalType) => Some(s"${v.toUnscaledLong}i64")
    case If(pred, t, f) =>
      for (p <- compilePred(pred, schema); a <- compileExpr(t, schema); b <- compileExpr(f, schema))
        yield s"(if $p $a $b)"
    case CaseWhen(branches, elseValue) =>
      compileCase(branches, elseValue, schema)
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

  /**
   * File-backed Average is compiled as sum+count so partitions can merge.
   * In-memory (no file leaf) still uses `(avg)` for a complete result.
   */
  private def compileAggNamed(
      e: NamedExpression,
      schema: Seq[Attribute]): Option[(Seq[String], Seq[NativeAggKind])] = {
    val child = e match {
      case Alias(c, _) => c
      case other => other
    }
    child match {
      case AggregateExpression(af, _, _, _, _) => compileAggFn(af, schema)
      case af: AggregateFunction => compileAggFn(af, schema)
      case other =>
        compileExpr(other, schema).map(s => (Seq(s), Seq(NativeAggKind.Pass)))
    }
  }

  private def compileAggFn(
      af: AggregateFunction,
      schema: Seq[Attribute]): Option[(Seq[String], Seq[NativeAggKind])] = af match {
    case Sum(c, _) =>
      compileExpr(c, schema).map(x => (Seq(s"(sum $x)"), Seq(NativeAggKind.Sum)))
    case Count(Nil) =>
      Some((Seq("(count)"), Seq(NativeAggKind.Count)))
    case Count(Seq(Literal(1, _))) =>
      Some((Seq("(count)"), Seq(NativeAggKind.Count)))
    case Count(Seq(c)) =>
      compileExpr(c, schema).map(x => (Seq(s"(count $x)"), Seq(NativeAggKind.Count)))
    case Min(c) =>
      compileExpr(c, schema).map(x => (Seq(s"(min $x)"), Seq(NativeAggKind.Min)))
    case Max(c) =>
      compileExpr(c, schema).map(x => (Seq(s"(max $x)"), Seq(NativeAggKind.Max)))
    case Average(c, _) =>
      compileExpr(c, schema).map { x =>
        (Seq(s"(sum $x)", s"(count $x)"), Seq(NativeAggKind.AvgSum, NativeAggKind.AvgCnt))
      }
    case _ => None
  }

  private def compileCase(
      branches: Seq[(Expression, Expression)],
      elseValue: Option[Expression],
      schema: Seq[Attribute]): Option[String] = {
    val elseIr = elseValue.flatMap(compileExpr(_, schema)).getOrElse("0i64")
    branches.foldRight(Option(elseIr)) { case ((p, t), acc) =>
      for {
        a <- acc
        pred <- compilePred(p, schema)
        thn <- compileExpr(t, schema)
      } yield s"(if $pred $thn $a)"
    }
  }

  private def isIgnorablePred(e: Expression): Boolean = e match {
    case IsNotNull(_) => true
    case And(l, r) => isIgnorablePred(l) && isIgnorablePred(r)
    case _ => false
  }

  private def predOnOneFileLeaf(e: Expression, c: Compiled): Boolean = {
    val refs = e.references
    if (refs.isEmpty) {
      false
    } else {
      val outs = if (c.leafOutputs.nonEmpty) c.leafOutputs
      else c.leaves.collect { case FileLeaf(p) => p.output }
      outs.count(o => refs.subsetOf(AttributeSet(o))) == 1
    }
  }

  private def attachPredsToLeaves(c: Compiled, preds: Seq[Expression]): Seq[LeafData] = {
    if (preds.isEmpty) {
      c.leaves
    } else {
      val outs = if (c.leafOutputs.nonEmpty) c.leafOutputs
      else c.leaves.collect { case FileLeaf(p) => p.output }
      c.leaves.zipWithIndex.map {
        case (FileLeaf(p), i) =>
          val mine = preds.filter(pr => pr.references.subsetOf(AttributeSet(outs(i))))
          if (mine.isEmpty) FileLeaf(p) else FileLeaf(Filter(mine.reduce(And), p))
        case (other, _) => other
      }
    }
  }

  def hash64(s: UTF8String): Long = {
    if (s == null) {
      0L
    } else {
      val h = (s.hashCode.toLong << 32) ^
        java.lang.Long.rotateLeft(s.numBytes().toLong + 1L, 17) ^ s.getPrefix
      if (h == 0L) 1L else h
    }
  }

  private def bin(op: String, l: Expression, r: Expression, schema: Seq[Attribute]): Option[String] =
    for (a <- compileExpr(l, schema); b <- compileExpr(r, schema)) yield s"($op $a $b)"

  private def colIndex(ref: String): Int = ref.drop(1).toInt

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
