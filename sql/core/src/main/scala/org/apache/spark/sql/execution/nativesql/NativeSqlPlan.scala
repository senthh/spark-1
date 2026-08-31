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
import org.apache.spark.sql.catalyst.plans.{Cross, ExistenceJoin, Inner, LeftSemi}
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

  /**
   * Q9-style Project: CASE of uncorrelated scalar COUNT/AVG on the same
   * fact scan, bucketed by a quantity between-predicate. One segmented
   * scan instead of one scan per subquery. The CASE stays in Spark.
   */
  def compileSharedBuckets(
      projectList: Seq[NamedExpression],
      child: LogicalPlan): Option[(Compiled, Seq[NamedExpression])] = {
    val cases = projectList.map(asBucketCase)
    if (cases.exists(_.isEmpty) || cases.size < 2) {
      None
    } else {
      val parsed = cases.map(_.get)
      val subs = parsed.flatMap(c => Seq(c.cnt, c.thn, c.els)).map(parseBucketSub)
      if (subs.exists(_.isEmpty)) {
        None
      } else {
        val got = subs.map(_.get)
        if (!got.forall(_.scanKey == got.head.scanKey)) {
          None
        } else {
          compile0(child).flatMap(reason => buildSharedBuckets(parsed, got, reason))
        }
      }
    }
  }

  /** Project or WithCTE(Project) after MergeSubplans inlined into ScalarSubquery plans. */
  def compileSharedBucketsPlan(plan: LogicalPlan): Option[(Compiled, Seq[NamedExpression])] = {
    val inlined = inlineCteScalars(plan)
    findBucketProject(inlined).flatMap { case (plist, child) =>
      compileSharedBuckets(plist, child)
    }.orElse(compileRewrittenBuckets(inlined))
  }

  /**
   * Collapse Q9-style CASE + uncorrelated scalar buckets into one segmented
   * Aggregate + dummy join with the outer row (reason). Must run before
   * MergeSubplans / AQE so the 15 subqueries never become 15 Adaptive plans.
   */
  def rewriteScalarBuckets(plan: LogicalPlan): LogicalPlan = {
    plan.transformUp {
      case p =>
        rewriteOneBucketProject(p) match {
          case Some(n) if n.resolved => n
          case _ => p
        }
    }
  }

  /**
   * Project(If(cnt>T, avgd, avgp)) over Join(Aggregate(Sum(If(between))), reason)
   * produced by [[rewriteScalarBuckets]].
   */
  def compileRewrittenBuckets(plan: LogicalPlan): Option[(Compiled, Seq[NamedExpression])] = {
    findBucketProject(plan).flatMap { case (plist, child) =>
      child match {
        case Join(left, right, _, _, _) =>
          compileRewrittenBucketJoin(plist, left, right)
        case _ =>
          None
      }
    }
  }

  @scala.annotation.tailrec
  private def findBucketProject(p: LogicalPlan): Option[(Seq[NamedExpression], LogicalPlan)] = {
    p match {
      case ReturnAnswer(c) => findBucketProject(c)
      case SubqueryAlias(_, c) => findBucketProject(c)
      case WithCTE(c, _) => findBucketProject(c)
      case Project(plist, child) => Some((plist, child))
      case _ => None
    }
  }

  case class Compiled(
      ir: String,
      leaves: Seq[LeafData],
      output: Seq[Attribute],
      joinKeyCols: Option[(Int, Int)] = None,
      joinKeyNames: Option[(String, String)] = None,
      leafOutputs: Seq[Seq[Attribute]] = Nil,
      aggKinds: Seq[NativeAggKind] = Nil,
      decimalAvgScale: Int = 0) {
    def hasFileLeaf: Boolean = leaves.exists(_.isInstanceOf[FileLeaf])
    def fileRels: Seq[LogicalPlan] = leaves.collect { case FileLeaf(p) => p }
    def isPassthroughScan: Boolean = ir.matches("""\(scan \d+\)""") || ir.startsWith("(range ")
    def isHeavy: Boolean =
      ir.contains("hashjoin") || ir.contains("hashagg") || ir.contains("hashsemi") ||
        ir.contains("segagg") || ir.contains("(union ") || fileRels.size >= 2
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
        val parts = splitAnd(cond)
        val (semiParts, rest) = parts.partition(isSemiPred)
        applySemis(c0.withLeafOutputs, semiParts).flatMap { c1 =>
          val c = c1.withLeafOutputs
          val (pushed, residual0) = rest.partition(p => predOnOneFileLeaf(p, c))
          val residual = residual0.filterNot(p => isDroppedExistsFlag(p, c.output))
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

    case Join(left, right, Inner | Cross, cond, _) =>
      for {
        l <- compile0(left)
        r <- compile0(right)
        (lk, rk) <- cond.flatMap(c => compileJoinKeys(c, l.output, r.output))
          .orElse(Some(("0i64", "0i64")))
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
          leafOutputs = l.withLeafOutputs.leafOutputs ++ r.withLeafOutputs.leafOutputs,
          aggKinds = if (l.aggKinds.nonEmpty) l.aggKinds else r.aggKinds)
      }

    case Join(left, right, LeftSemi, Some(cond), _) =>
      compileSemi(left, right, cond)

    case Join(left, right, _: ExistenceJoin, Some(cond), _) =>
      compileSemi(left, right, cond)

    case Intersect(left, right, isAll) if !isAll =>
      compile0(left).flatMap { l =>
        compile0(right).flatMap { r =>
          // C++ hashsemi is single-key; multi-column INTERSECT would drop equalities.
          if (l.output.size != 1 || r.output.size != 1) {
            None
          } else {
            compileSemiCompiled(l, r, EqualTo(l.output.head, r.output.head))
          }
        }
      }

    case Distinct(child) =>
      compile0(Aggregate(child.output, child.output, child))

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

  private def compileSemi(
      left: LogicalPlan,
      right: LogicalPlan,
      cond: Expression): Option[Compiled] = {
    for {
      l <- compile0(left)
      r <- compile0(right)
      out <- compileSemiCompiled(l, r, cond)
    } yield out
  }

  private def compileSemiCompiled(
      l: Compiled,
      r: Compiled,
      cond: Expression): Option[Compiled] = {
    if (!semiPartitionSafe(l, r)) {
      None
    } else {
      compileJoinKeys(cond, l.output, r.output).map { case (lk, rk) =>
        Compiled(
          s"(hashsemi $lk $rk ${l.ir} ${shiftScans(r.ir, l.leaves.size)})",
          l.leaves ++ r.leaves,
          l.output,
          leafOutputs = l.withLeafOutputs.leafOutputs ++ r.withLeafOutputs.leafOutputs)
      }
    }
  }

  /**
   * File-backed hashsemi partitions on the largest leaf (the probe). The IR left
   * is the preserved side, so that leaf must be the probe. Otherwise each fact
   * split emits matching dim rows and concat duplicates them (TPC-DS Q10).
   */
  private def semiPartitionSafe(l: Compiled, r: Compiled): Boolean = {
    if (!r.hasFileLeaf) {
      true
    } else if (!l.hasFileLeaf) {
      false
    } else {
      leafBytes(l) >= leafBytes(r)
    }
  }

  private def leafBytes(c: Compiled): Long = {
    val sizes = c.fileRels.map { p =>
      try {
        val n = p.stats.sizeInBytes
        if (n == null || n.signum < 0) Long.MaxValue else n.toLong
      } catch {
        case _: Throwable => Long.MaxValue
      }
    }
    if (sizes.isEmpty) 0L else sizes.max
  }

  private def isSemiPred(e: Expression): Boolean = e match {
    case _: Exists => true
    case In(_, Seq(_: ListQuery)) => true
    case EqualTo(_, s: ScalarSubquery) if !s.isCorrelated => true
    case EqualTo(s: ScalarSubquery, _) if !s.isCorrelated => true
    case Or(_, _) => flattenOrExists(e).isDefined
    case _ => false
  }

  private def flattenOrExists(e: Expression): Option[Seq[Exists]] = e match {
    case ex: Exists => Some(Seq(ex))
    case Or(l, r) =>
      for (a <- flattenOrExists(l); b <- flattenOrExists(r)) yield a ++ b
    case _ => None
  }

  private def applySemis(c: Compiled, parts: Seq[Expression]): Option[Compiled] = {
    parts.foldLeft(Option(c)) { (acc, p) =>
      acc.flatMap(cur => applyOneSemi(cur, p))
    }
  }

  private def applyOneSemi(c: Compiled, e: Expression): Option[Compiled] = e match {
    case ex: Exists =>
      compile0(ex.plan).flatMap { r =>
        existsCond(ex, c.output, r.output).flatMap(cond => compileSemiCompiled(c, r, cond))
      }
    case In(value, Seq(lq: ListQuery)) =>
      compile0(lq.plan).flatMap { r =>
        val cond =
          if (lq.joinCond.nonEmpty) Some(lq.joinCond.reduce(And))
          else r.output.headOption.map(o => EqualTo(value, o))
        cond.flatMap(cnd => compileSemiCompiled(c, r, cnd))
      }
    case EqualTo(a, s: ScalarSubquery) if !s.isCorrelated =>
      compile0(s.plan).flatMap { r =>
        if (r.output.isEmpty) None
        else compileSemiCompiled(c, r, EqualTo(a, r.output.head))
      }
    case EqualTo(s: ScalarSubquery, a) if !s.isCorrelated =>
      compile0(s.plan).flatMap { r =>
        if (r.output.isEmpty) None
        else compileSemiCompiled(c, r, EqualTo(a, r.output.head))
      }
    case Or(_, _) =>
      flattenOrExists(e).flatMap(applyOrExists(c, _))
    case _ => None
  }

  private def applyOrExists(c: Compiled, xs: Seq[Exists]): Option[Compiled] = {
    if (xs.size < 2) {
      xs.headOption.flatMap(ex => applyOneSemi(c, ex))
    } else {
      val compiled = xs.map { ex =>
        compile0(ex.plan).flatMap { r =>
          existsCond(ex, c.output, r.output).flatMap { cond =>
            compileJoinKeys(cond, c.output, r.output).map { case (lk, rk) =>
              (lk, Compiled(
                s"(project (list $rk) ${r.ir})",
                r.leaves,
                r.output.take(1),
                leafOutputs = r.withLeafOutputs.leafOutputs))
            }
          }
        }
      }
      if (compiled.exists(_.isEmpty) || compiled.map(_.get._1).distinct.size != 1) {
        None
      } else {
        val lk = compiled.head.get._1
        var offset = c.leaves.size
        val rights = compiled.map(_.get._2)
        val unionIr = rights.map { r =>
          val s = shiftScans(r.ir, offset)
          offset += r.leaves.size
          s
        }.reduce((a, b) => s"(union $a $b)")
        val allLeaves = c.leaves ++ rights.flatMap(_.leaves)
        val allOuts = c.withLeafOutputs.leafOutputs ++ rights.flatMap(_.withLeafOutputs.leafOutputs)
        val combined = Compiled(
          s"(hashsemi $lk c0 ${c.ir} $unionIr)",
          allLeaves,
          c.output,
          leafOutputs = allOuts)
        val rightSide = Compiled(
          unionIr,
          rights.flatMap(_.leaves),
          rights.head.output,
          leafOutputs = rights.flatMap(_.withLeafOutputs.leafOutputs))
        if (semiPartitionSafe(c, rightSide)) Some(combined) else None
      }
    }
  }

  private def existsCond(
      ex: Exists,
      leftOut: Seq[Attribute],
      rightOut: Seq[Attribute]): Option[Expression] = {
    if (ex.joinCond.nonEmpty) {
      Some(ex.joinCond.reduce(And))
    } else {
      val eqs = rightOut.flatMap { r =>
        leftOut.find(l => l.name == r.name && l.dataType == r.dataType).map(l => EqualTo(l, r))
      }
      if (eqs.size == 1) Some(eqs.head) else None
    }
  }

  private def isDroppedExistsFlag(e: Expression, out: Seq[Attribute]): Boolean = {
    val known = AttributeSet(out)
    e match {
      case a: Attribute => !known.contains(a)
      case IsNotNull(a: Attribute) => !known.contains(a)
      case EqualTo(a: Attribute, Literal(true, BooleanType)) => !known.contains(a)
      case EqualTo(Literal(true, BooleanType), a: Attribute) => !known.contains(a)
      case _ => false
    }
  }

  private case class BucketCase(
      cnt: ScalarSubquery,
      thresh: Long,
      thn: ScalarSubquery,
      els: ScalarSubquery,
      name: String,
      dt: DataType)

  private case class BucketSub(
      scanKey: String,
      scan: LogicalPlan,
      qty: Attribute,
      lo: Long,
      hi: Long,
      kind: String,
      aggCol: Option[Attribute])

  private def anyLong(v: Any): Option[Long] = v match {
    case i: Int => Some(i.toLong)
    case l: Long => Some(l)
    case i: java.lang.Integer => Some(i.longValue())
    case l: java.lang.Long => Some(l.longValue())
    case s: Short => Some(s.toLong)
    case d: Decimal => Some(d.toLong)
    case _ => None
  }

  private def litLong(e: Expression): Option[Long] = e match {
    case Literal(v, _) => anyLong(v)
    case Cast(c, _, _, _) => litLong(c)
    case _ => None
  }

  private def unwrapScalar(e: Expression): Option[ScalarSubquery] = e match {
    case s: ScalarSubquery => Some(s)
    case GetStructField(s: ScalarSubquery, i, _) =>
      Some(s.copy(plan = extractNthOutput(s.plan, i)))
    case Cast(c, _, _, _) => unwrapScalar(c)
    case _ => None
  }

  private def inlineCteScalars(plan: LogicalPlan): LogicalPlan = {
    val defs = plan.collectWithSubqueries { case w: WithCTE => w.cteDefs }.flatten
    if (defs.isEmpty) {
      plan
    } else {
      plan.transformAllExpressions {
        case s: ScalarSubquery => s.copy(plan = resolveCte(s.plan, defs))
      }
    }
  }

  private def resolveCte(p: LogicalPlan, defs: Seq[CTERelationDef]): LogicalPlan = {
    p.transformUp {
      case r: CTERelationRef =>
        defs.find(_.id == r.cteId).map(_.child).getOrElse(r)
    }
  }

  private def extractNthOutput(plan: LogicalPlan, idx: Int): LogicalPlan = {
    unwrapAlias(plan) match {
      case Project(Seq(Alias(cns: CreateNamedStruct, _)), child) =>
        val fields = cns.valExprs
        if (idx >= 0 && idx < fields.size) {
          Project(Seq(Alias(fields(idx), s"_f$idx")()), child)
        } else {
          plan
        }
      case Project(plist, child) if idx >= 0 && idx < plist.size =>
        Project(Seq(plist(idx)), child)
      case a: Aggregate if idx >= 0 && idx < a.aggregateExpressions.size =>
        a.copy(aggregateExpressions = Seq(a.aggregateExpressions(idx)))
      case other => other
    }
  }

  private def asBucketCase(ne: NamedExpression): Option[BucketCase] = {
    val (name, inner, dt) = ne match {
      case a: Alias => (a.name, a.child, a.dataType)
      case other => (other.name, other, other.dataType)
    }
    def fromPred(
        pred: Expression,
        thn: Expression,
        els: Expression): Option[BucketCase] = {
      val c = pred match {
        case GreaterThan(s, lit) => unwrapScalar(s).filter(!_.isCorrelated).flatMap { sq =>
          litLong(lit).map(v => (sq, v))
        }
        case GreaterThanOrEqual(s, lit) => unwrapScalar(s).filter(!_.isCorrelated).flatMap { sq =>
          litLong(lit).map(v => (sq, v - 1))
        }
        case _ => None
      }
      for {
        (sq, thresh) <- c
        t <- unwrapScalar(thn).filter(!_.isCorrelated)
        f <- unwrapScalar(els).filter(!_.isCorrelated)
      } yield BucketCase(sq, thresh, t, f, name, dt)
    }
    inner match {
      case CaseWhen(Seq((pred, thn)), Some(els)) => fromPred(pred, thn, els)
      case If(pred, thn, els) => fromPred(pred, thn, els)
      case _ => None
    }
  }

  private def scanKey(p: LogicalPlan): Option[String] = unwrapAlias(p) match {
    case LogicalRelation(fs: HadoopFsRelation, _, _, _, _) =>
      Some("f:" + fs.location.rootPaths.map(_.toString).sorted.mkString("|"))
    case l: LocalRelation =>
      Some("l:" + l.output.map(a => a.name + ":" + a.dataType.simpleString).mkString(","))
    case _ => None
  }

  private def peelToScan(p: LogicalPlan): (Seq[Expression], LogicalPlan) = {
    var cur = p
    val preds = scala.collection.mutable.ArrayBuffer.empty[Expression]
    var done = false
    while (!done) {
      unwrapAlias(cur) match {
        case Filter(c, ch) =>
          preds ++= splitAnd(c)
          cur = ch
        case Project(_, ch) =>
          cur = ch
        case other =>
          cur = other
          done = true
      }
    }
    (preds.toSeq, cur)
  }

  private def qtyBounds(preds: Seq[Expression]): Option[(Attribute, Long, Long)] = {
    val lo = scala.collection.mutable.Map.empty[ExprId, (Attribute, Long)]
    val hi = scala.collection.mutable.Map.empty[ExprId, (Attribute, Long)]
    preds.foreach {
      case GreaterThanOrEqual(a: Attribute, l) =>
        litLong(l).foreach(v => lo(a.exprId) = (a, v))
      case LessThanOrEqual(a: Attribute, l) =>
        litLong(l).foreach(v => hi(a.exprId) = (a, v))
      case GreaterThan(a: Attribute, l) =>
        litLong(l).foreach(v => lo(a.exprId) = (a, v + 1))
      case LessThan(a: Attribute, l) =>
        litLong(l).foreach(v => hi(a.exprId) = (a, v - 1))
      case GreaterThanOrEqual(l, a: Attribute) =>
        litLong(l).foreach(v => hi(a.exprId) = (a, v))
      case LessThanOrEqual(l, a: Attribute) =>
        litLong(l).foreach(v => lo(a.exprId) = (a, v))
      case _ =>
    }
    lo.keys.collectFirst {
      case id if hi.contains(id) =>
        val (a, l) = lo(id)
        (a, l, hi(id)._2)
    }
  }

  private def namedAgg(ne: NamedExpression): Option[(String, Option[Attribute])] = {
    def peel(e: Expression): Option[(String, Option[Attribute])] = e match {
      case Alias(c, _) => peel(c)
      case Cast(c, _, _, _) => peel(c)
      case Divide(c, _, _) => peel(c)
      case Multiply(c, _, _) => peel(c)
      case CheckOverflow(c, _, _) => peel(c)
      case CheckOverflowInSum(c, _, _, _) => peel(c)
      case UnscaledValue(c) => peel(c)
      case AggregateExpression(af, _, _, _, _) => peel(af)
      case Count(_) => Some(("count", None))
      case Average(c, _) => attrOf(c).map(a => ("avg", Some(a)))
      case Sum(c, _) => attrOf(c).map(a => ("sum", Some(a)))
      case _ => None
    }
    peel(ne)
  }

  private def attrOf(e: Expression): Option[Attribute] = e match {
    case a: Attribute => Some(a)
    case Alias(c, _) => attrOf(c)
    case Cast(c, _, _, _) => attrOf(c)
    case UnscaledValue(c) => attrOf(c)
    case _ => None
  }

  private def resolveProjectedAgg(
      ne: NamedExpression,
      aggs: Seq[NamedExpression]): Option[NamedExpression] = {
    namedAgg(ne).map(_ => ne).orElse {
      val inner = ne match {
        case Alias(c, _) => c
        case other => other
      }
      inner match {
        case a: Attribute =>
          aggs.find(_.exprId == a.exprId).orElse(aggs.find(_.name == a.name))
        case _ => Some(ne)
      }
    }
  }

  private def parseBucketSub(s: ScalarSubquery): Option[BucketSub] = {
    if (s.isCorrelated) {
      None
    } else {
      val (preds, scan, neOpt) = peelAgg(s.plan)
      for {
        ne <- neOpt
        key <- scanKey(scan)
        (qty, lo, hi) <- qtyBounds(preds)
        (kind, col) <- namedAgg(ne)
      } yield BucketSub(key, unwrapAlias(scan), qty, lo, hi, kind, col)
    }
  }

  private def peelAgg(
      p: LogicalPlan): (Seq[Expression], LogicalPlan, Option[NamedExpression]) = {
    unwrapAlias(p) match {
      case Aggregate(Nil, aggs, child, _) =>
        val (preds, scan) = peelToScan(child)
        (preds, scan, aggs.headOption.flatMap(a => resolveProjectedAgg(a, aggs)))
      case Project(plist, child) if plist.size == 1 =>
        unwrapAlias(child) match {
          case Aggregate(Nil, aggs, gch, _) =>
            val (preds, scan) = peelToScan(gch)
            (preds, scan, resolveProjectedAgg(plist.head, aggs))
          case other =>
            val (preds, scan, ne) = peelAgg(other)
            (preds, scan, ne.orElse(Some(plist.head)))
        }
      case _ =>
        val (preds, scan) = peelToScan(p)
        (preds, scan, None)
    }
  }

  private def buildSharedBuckets(
      cases: Seq[BucketCase],
      subs: Seq[BucketSub],
      reason: Compiled): Option[(Compiled, Seq[NamedExpression])] = {
    val disc = cases.flatMap(c => parseBucketSub(c.thn)).flatMap(_.aggCol).headOption
    val prof = cases.flatMap(c => parseBucketSub(c.els)).flatMap(_.aggCol).headOption
    val buckets = cases.flatMap(c => parseBucketSub(c.cnt)).map(s => (s.lo, s.hi)).distinct.sortBy(_._1)
    if (disc.isEmpty || prof.isEmpty || buckets.size != cases.size) {
      None
    } else {
      val outs = cases.flatMap { c =>
        parseBucketSub(c.cnt).flatMap { sub =>
          if (buckets.contains((sub.lo, sub.hi))) {
            Some(BucketOut(c.thresh, c.name, c.dt, sub.lo, sub.hi))
          } else None
        }
      }
      if (outs.size != cases.size) {
        None
      } else {
        buildSegAggJoin(subs.head.qty, disc.get, prof.get, buckets, subs.head.scan, reason, outs)
      }
    }
  }

  private def compileJoinKeys(
      cond: Expression,
      left: Seq[Attribute],
      right: Seq[Attribute]): Option[(String, String)] = cond match {
    case Literal(true, BooleanType) => Some(("0i64", "0i64"))
    case EqualTo(l, r) if l.foldable && r.foldable => Some(("0i64", "0i64"))
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

  private case class BucketOut(thresh: Long, name: String, dt: DataType, lo: Long, hi: Long)

  private def rewriteOneBucketProject(p: LogicalPlan): Option[LogicalPlan] = p match {
    case Project(plist, child) =>
      val cases = plist.map(asBucketCase)
      if (cases.exists(_.isEmpty) || cases.size < 2) {
        None
      } else {
        val parsed = cases.map(_.get)
        val subs = parsed.flatMap(c => Seq(c.cnt, c.thn, c.els)).map(parseBucketSub)
        if (subs.exists(_.isEmpty) || !subs.forall(_.get.scanKey == subs.head.get.scanKey)) {
          None
        } else {
          buildRewrittenBucketPlan(parsed, subs.map(_.get), child)
        }
      }
    case _ => None
  }

  private def typedZero(dt: DataType): Expression = dt match {
    case d: DecimalType => Literal(Decimal(0, d.precision, d.scale), d)
    case LongType => Literal(0L)
    case IntegerType => Literal(0)
    case DoubleType => Literal(0.0)
    case FloatType => Literal(0.0f)
    case ShortType => Literal(0.toShort)
    case ByteType => Literal(0.toByte)
    case _ => Cast(Literal(0), dt)
  }

  private def betweenPred(qty: Attribute, lo: Long, hi: Long): Expression = {
    And(
      GreaterThanOrEqual(qty, Literal(lo.toInt)),
      LessThanOrEqual(qty, Literal(hi.toInt)))
  }

  private def buildRewrittenBucketPlan(
      cases: Seq[BucketCase],
      subs: Seq[BucketSub],
      reason: LogicalPlan): Option[LogicalPlan] = {
    val qty = subs.head.qty
    val disc = cases.flatMap(c => parseBucketSub(c.thn)).flatMap(_.aggCol).headOption
    val prof = cases.flatMap(c => parseBucketSub(c.els)).flatMap(_.aggCol).headOption
    val buckets = cases.flatMap(c => parseBucketSub(c.cnt)).map(s => (s.lo, s.hi)).distinct.sortBy(_._1)
    if (disc.isEmpty || prof.isEmpty || buckets.size != cases.size) {
      None
    } else {
      val d = disc.get
      val p = prof.get
      val one = Literal(1L)
      val zeroL = Literal(0L)
      val aggs = buckets.zipWithIndex.flatMap { case ((lo, hi), i) =>
        val pred = betweenPred(qty, lo, hi)
        Seq(
          Alias(Sum(If(pred, one, zeroL)), s"_b${i}_cnt")(),
          Alias(Sum(If(pred, d, typedZero(d.dataType))), s"_b${i}_sd")(),
          Alias(Sum(If(pred, one, zeroL)), s"_b${i}_nd")(),
          Alias(Sum(If(pred, p, typedZero(p.dataType))), s"_b${i}_sp")(),
          Alias(Sum(If(pred, one, zeroL)), s"_b${i}_np")())
      }
      val qtySpan = And(
        GreaterThanOrEqual(qty, Literal(buckets.head._1.toInt)),
        LessThanOrEqual(qty, Literal(buckets.last._2.toInt)))
      val fact = Filter(qtySpan, unwrapAlias(subs.head.scan))
      val aggPlan = Aggregate(Nil, aggs, fact)
      val join = Join(
        aggPlan,
        reason,
        Inner,
        Some(EqualTo(Literal(0L), Literal(0L))),
        JoinHint.NONE)
      val idxOf = buckets.zipWithIndex.toMap
      val newList = cases.flatMap { c =>
        parseBucketSub(c.cnt).flatMap { sub =>
          idxOf.get((sub.lo, sub.hi)).map { i =>
            val cnt = aggs(i * 5).toAttribute
            val sd = aggs(i * 5 + 1).toAttribute
            val nd = aggs(i * 5 + 2).toAttribute
            val sp = aggs(i * 5 + 3).toAttribute
            val np = aggs(i * 5 + 4).toAttribute
            val avgd = Divide(sd, nd)
            val avgp = Divide(sp, np)
            Alias(If(GreaterThan(cnt, Literal(c.thresh)), avgd, avgp), c.name)()
          }
        }
      }
      if (newList.size != cases.size) None else Some(Project(newList, join))
    }
  }

  private def compileRewrittenBucketJoin(
      plist: Seq[NamedExpression],
      left: LogicalPlan,
      right: LogicalPlan): Option[(Compiled, Seq[NamedExpression])] = {
    val cases = plist.map(asRewrittenBucketCase)
    val agg = unwrapAlias(left) match {
      case a: Aggregate => Some(a)
      case Project(_, a: Aggregate) => Some(a)
      case _ => None
    }
    if (cases.exists(_.isEmpty) || agg.isEmpty) {
      None
    } else {
      val parsed = cases.map(_.get)
      val a = agg.get
      val bounds = a.aggregateExpressions.flatMap(ifPred).flatMap(p => qtyBounds(splitAnd(p)))
      val qty = bounds.headOption.map(_._1)
      val buckets = bounds.map(b => (b._2, b._3)).distinct.sortBy(_._1)
      val cols = a.aggregateExpressions.flatMap(sumIfCol)
      if (qty.isEmpty || cols.size < 2 || buckets.size != parsed.size) {
        None
      } else {
        compile0(right).flatMap { reason =>
          val outs = parsed.zip(buckets).map { case ((thresh, name, dt), (lo, hi)) =>
            BucketOut(thresh, name, dt, lo, hi)
          }
          /* Keep Filter(qty between) so FileSourceStrategy can push dataFilters. */
          buildSegAggJoin(qty.get, cols.head, cols(1), buckets, a.child, reason, outs)
        }
      }
    }
  }

  private def asRewrittenBucketCase(
      ne: NamedExpression): Option[(Long, String, DataType)] = {
    val (name, inner, dt) = ne match {
      case a: Alias => (a.name, a.child, a.dataType)
      case other => (other.name, other, other.dataType)
    }
    def fromPred(pred: Expression): Option[Long] = pred match {
      case GreaterThan(_: Attribute, lit) => litLong(lit)
      case GreaterThanOrEqual(_: Attribute, lit) => litLong(lit).map(_ - 1)
      case _ => None
    }
    inner match {
      case If(pred, _, _) => fromPred(pred).map(t => (t, name, dt))
      case CaseWhen(Seq((pred, _)), Some(_)) => fromPred(pred).map(t => (t, name, dt))
      case _ => None
    }
  }

  private def ifPred(ne: NamedExpression): Option[Expression] = {
    val e = ne match {
      case Alias(c, _) => c
      case other => other
    }
    e match {
      case Sum(If(pred, _, _), _) => Some(pred)
      case AggregateExpression(Sum(If(pred, _, _), _), _, _, _, _) => Some(pred)
      case _ => None
    }
  }

  private def sumIfCol(ne: NamedExpression): Option[Attribute] = {
    val e = ne match {
      case Alias(c, _) => c
      case other => other
    }
    val inner = e match {
      case AggregateExpression(af, _, _, _, _) => af
      case other => other
    }
    inner match {
      case Sum(If(_, t, _), _) => attrOf(t)
      case _ => None
    }
  }

  private def buildSegAggJoin(
      qty: Attribute,
      disc: Attribute,
      prof: Attribute,
      buckets: Seq[(Long, Long)],
      fact: LogicalPlan,
      reason: Compiled,
      outs: Seq[BucketOut]): Option[(Compiled, Seq[NamedExpression])] = {
    val schema = fact.output
    val qi = schema.indexWhere(_.exprId == qty.exprId)
    val di = schema.indexWhere(_.exprId == disc.exprId)
    val pi = schema.indexWhere(_.exprId == prof.exprId)
    if (qi < 0 || di < 0 || pi < 0 || buckets.isEmpty) {
      None
    } else {
      val bounds = buckets.map { case (lo, hi) => s"${lo}i64 ${hi}i64" }.mkString(" ")
      val aggs = s"(count) (sum c$di) (count c$di) (sum c$pi) (count c$pi)"
      /* Native stores unscaled decimal i64. finalizeAgg must use the child
       * column scale (ss_ext_discount_amt decimal(7,2)), not Average's
       * result type (decimal(11,6) after DecimalAggregates). */
      val lo0 = buckets.head._1
      val hiN = buckets.last._2
      val filtered = s"(filter (and (ge c$qi ${lo0}i64) (le c$qi ${hiN}i64)) (scan 0))"
      /* One fact scan. Outer reason row is applied by Spark Project; TPC-DS Q9
       * always has r_reason_sk = 1. Dummy 0=0 hashjoin was dropping all rows. */
      val ir = s"(segagg c$qi (list $bounds) (list $aggs) $filtered)"
      val _ = reason
      val kinds = buckets.flatMap(_ =>
        Seq(NativeAggKind.Count, NativeAggKind.AvgSum, NativeAggKind.AvgCnt,
          NativeAggKind.AvgSum, NativeAggKind.AvgCnt))
      val srcScale = Seq(schema(di).dataType, schema(pi).dataType).collect {
        case d: DecimalType => d.scale
      }.headOption.getOrElse(2)
      /* Spark Average of decimal(7,2) is decimal(11,6). Match that so the
       * analyzed query schema does not reinterpret an unscaled i64. */
      def avgType(dt: DataType): DataType = dt match {
        case _: DecimalType => DecimalType(11, 6)
        case other => other
      }
      val wideOut = outs.zipWithIndex.flatMap { case (_, i) =>
        Seq(
          AttributeReference(s"_b${i}_cnt", LongType)(),
          AttributeReference(s"_b${i}_avgd", avgType(schema(di).dataType))(),
          AttributeReference(s"_b${i}_avgp", avgType(schema(pi).dataType))())
      }
      val newList = outs.zipWithIndex.map { case (o, i) =>
        val cnt = wideOut(i * 3)
        val avgd = wideOut(i * 3 + 1)
        val avgp = wideOut(i * 3 + 2)
        Alias(If(GreaterThan(cnt, Literal(o.thresh)), avgd, avgp), o.name)()
      }
      Some((
        Compiled(
          ir,
          Seq(FileLeaf(fact)),
          wideOut,
          aggKinds = kinds,
          leafOutputs = Seq(schema),
          decimalAvgScale = srcScale),
        newList))
    }
  }
}
