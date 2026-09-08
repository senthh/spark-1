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

package org.apache.spark.sql.vegam.exec

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkException
import org.apache.spark.sql.vegam.plan.{
  AggCall, BuildJoin, CountStar, ExpandSlot, ExpandSpec, FileRef, HashAgg, NativePlan,
  StagePlan, WinSpec}

/**
 * IR interpreter used when libvegam is not loaded. Same [[NativePlan]] as
 * the C++ engine. Production path is [[NativeBackend]].
 */
object JvmBackend extends VegamBackend {

  override def createTask(plan: NativePlan): VegamTask = new JvmTask(plan)

  private class JvmTask(plan: NativePlan) extends VegamTask {
    private var done = false

    override def nextPage(): Option[VegamPage] = {
      if (done) {
        None
      } else {
        done = true
        Some(run(plan))
      }
    }

    override def close(): Unit = {
      done = true
    }
  }

  private def run(plan: NativePlan): VegamPage = plan match {
    case CountStar(files) =>
      val total = files.map(ParquetIO.footerRows).sum
      VegamPage(1, 1, Array(total.toDouble), Array(false), Array(0))
    case h: HashAgg =>
      hashAgg(h)
    case s: StagePlan =>
      stage(s)
    case other =>
      throw SparkException.internalError(
        s"vegam jvm backend cannot run ${other.getClass.getName}")
  }

  private def hashAgg(h: HashAgg): VegamPage = {
    val want = (h.groups ++ h.aggCalls.map(_.col).filter(_.nonEmpty)).distinct
    val table = readAll(h.refs, want, h.allFilters)
    aggTable(table, h.groups, h.aggCalls)
  }

  private def stage(s: StagePlan): VegamPage = {
    val probeWant = (
      s.groups ++
        s.aggs.map(_.col) ++
        s.probeFilters.map(_.col) ++
        s.builds.flatMap(_.probeKeys) ++
        s.window.toSeq.flatMap(w => w.partitionBy ++ w.orderBy.map(_._1) ++ w.fns.map(_.col)) ++
        s.expand.toSeq.flatMap(_.projections.flatten.map(_.col)) ++
        s.probe.columns
      ).filter(_.nonEmpty).distinct
    var table = readAll(s.probe.files, probeWant, s.probeFilters)
    s.builds.foreach { b =>
      table = join(table, b)
    }
    s.expand.foreach { e =>
      table = expand(table, e)
    }
    s.window.foreach { w =>
      table = window(table, w)
    }
    if (s.aggs.nonEmpty || s.groups.nonEmpty) {
      aggTable(table, s.groups, s.aggs)
    } else {
      toPage(table)
    }
  }

  private def readAll(
      refs: Seq[FileRef],
      want: Seq[String],
      filters: Seq[org.apache.spark.sql.vegam.plan.FilterPred]): ParquetIO.Table = {
    val names = want.distinct.toArray
    val rows = new ArrayBuffer[Array[Any]]()
    refs.foreach { ref =>
      val t = ParquetIO.read(ref, names.toSeq, filters)
      val idx = names.map(n => t.colIndex(n))
      t.rows.foreach { r =>
        val out = new Array[Any](names.length)
        var i = 0
        while (i < names.length) {
          val j = idx(i)
          out(i) = if (j >= 0) r(j) else null
          i += 1
        }
        rows += out
      }
    }
    new ParquetIO.Table(names, rows)
  }

  private def join(probe: ParquetIO.Table, b: BuildJoin): ParquetIO.Table = {
    val buildWant = (b.buildKeys ++ b.filters.map(_.col) ++ b.scan.columns)
      .filter(_.nonEmpty).distinct
    val build = readAll(b.scan.files, buildWant, b.filters)
    val pIdx = b.probeKeys.map(probe.colIndex)
    val bIdx = b.buildKeys.map(build.colIndex)
    val index = new mutable.HashMap[String, ArrayBuffer[Array[Any]]]()
    build.rows.foreach { row =>
      val k = keyOf(row, bIdx)
      if (k != null) {
        index.getOrElseUpdate(k, new ArrayBuffer[Array[Any]]()) += row
      }
    }
    val extra = build.names.zipWithIndex.filterNot { case (n, _) =>
      probe.names.contains(n) || b.buildKeys.contains(n)
    }.toSeq
    val outNames = probe.names ++ extra.map(_._1)
    val out = new ArrayBuffer[Array[Any]]()
    probe.rows.foreach { prow =>
      val k = keyOf(prow, pIdx)
      val hits = if (k == null) None else index.get(k)
      b.joinType match {
        case NativePlan.JOIN_INNER =>
          hits.foreach(_.foreach(h => out += concat(prow, h, extra, outNames.length)))
        case NativePlan.JOIN_LEFT =>
          if (hits.isEmpty) {
            out += concat(prow, null, extra, outNames.length)
          } else {
            hits.get.foreach(h => out += concat(prow, h, extra, outNames.length))
          }
        case NativePlan.JOIN_SEMI =>
          if (hits.exists(_.nonEmpty)) {
            out += prow
          }
        case NativePlan.JOIN_ANTI =>
          if (hits.isEmpty) {
            out += prow
          }
        case _ =>
      }
    }
    val names = if (b.joinType == NativePlan.JOIN_SEMI || b.joinType == NativePlan.JOIN_ANTI) {
      probe.names
    } else {
      outNames
    }
    new ParquetIO.Table(names, out)
  }

  private def concat(
      probe: Array[Any],
      build: Array[Any],
      extra: Seq[(String, Int)],
      n: Int): Array[Any] = {
    val out = new Array[Any](n)
    Array.copy(probe, 0, out, 0, probe.length)
    if (build != null) {
      var i = 0
      while (i < extra.length) {
        out(probe.length + i) = build(extra(i)._2)
        i += 1
      }
    }
    out
  }

  private def keyOf(row: Array[Any], idx: Seq[Int]): String = {
    val b = new StringBuilder()
    var i = 0
    while (i < idx.length) {
      val v = if (idx(i) < 0) null else row(idx(i))
      if (v == null) {
        return null
      }
      if (i > 0) b.append('\u0001')
      b.append(v.toString)
      i += 1
    }
    b.toString
  }

  private def keyOfNulls(row: Array[Any], idx: Seq[Int]): String = {
    val b = new StringBuilder()
    var i = 0
    while (i < idx.length) {
      val v = if (idx(i) < 0) null else row(idx(i))
      if (i > 0) b.append('\u0001')
      if (v == null) b.append('\u0000') else b.append(v.toString)
      i += 1
    }
    b.toString
  }

  private def expand(table: ParquetIO.Table, spec: ExpandSpec): ParquetIO.Table = {
    val out = new ArrayBuffer[Array[Any]]()
    table.rows.foreach { row =>
      spec.projections.foreach { proj =>
        val n = new Array[Any](spec.outCols.length)
        var i = 0
        while (i < proj.length && i < n.length) {
          n(i) = expandSlot(table, row, proj(i))
          i += 1
        }
        out += n
      }
    }
    new ParquetIO.Table(spec.outCols.toArray, out)
  }

  private def expandSlot(table: ParquetIO.Table, row: Array[Any], s: ExpandSlot): Any = {
    s.kind match {
      case NativePlan.EXPAND_COL =>
        val i = table.colIndex(s.col)
        if (i >= 0) row(i) else null
      case NativePlan.EXPAND_NULL => null
      case NativePlan.EXPAND_LONG => s.lvalue
      case NativePlan.EXPAND_DOUBLE => s.dvalue
      case NativePlan.EXPAND_STR => s.svalue
      case _ => null
    }
  }

  private def window(table: ParquetIO.Table, w: WinSpec): ParquetIO.Table = {
    val partIdx = w.partitionBy.map(table.colIndex)
    val orderIdx = w.orderBy.map { case (c, asc) => (table.colIndex(c), asc) }
    val groups = table.rows.groupBy(r => keyOf(r, partIdx)).values
    val extraNames = w.fns.map(_.alias).toArray
    val names = table.names ++ extraNames
    val out = new ArrayBuffer[Array[Any]]()
    groups.foreach { buf =>
      val sorted = buf.sortInPlace()(ord(orderIdx))
      val sums = w.fns.map { f =>
        if (f.kind == NativePlan.WIN_SUM) {
          val i = table.colIndex(f.col)
          var s = 0.0
          sorted.foreach { r =>
            ParquetIO.toDouble(if (i >= 0) r(i) else null).foreach(s += _)
          }
          s
        } else {
          0.0
        }
      }
      var i = 0
      var prevKey: String = null
      var rank = 0
      var dense = 0
      while (i < sorted.length) {
        val row = sorted(i)
        val ok = keyOf(row, orderIdx.map(_._1))
        if (ok != prevKey) {
          rank = i + 1
          dense += 1
          prevKey = ok
        }
        val extra = w.fns.zipWithIndex.map { case (f, fi) =>
          f.kind match {
            case NativePlan.WIN_ROW_NUMBER => (i + 1).toLong
            case NativePlan.WIN_RANK => rank.toLong
            case NativePlan.WIN_DENSE_RANK => dense.toLong
            case NativePlan.WIN_SUM => sums(fi)
            case _ => null
          }
        }
        val n = new Array[Any](names.length)
        Array.copy(row, 0, n, 0, row.length)
        var j = 0
        while (j < extra.length) {
          n(row.length + j) = extra(j)
          j += 1
        }
        out += n
        i += 1
      }
    }
    new ParquetIO.Table(names, out)
  }

  private def ord(order: Seq[(Int, Boolean)]): Ordering[Array[Any]] = {
    (a: Array[Any], b: Array[Any]) => {
      var c = 0
      var i = 0
      while (c == 0 && i < order.length) {
        val (idx, asc) = order(i)
        val av = if (idx >= 0) a(idx) else null
        val bv = if (idx >= 0) b(idx) else null
        c = cmpCell(av, bv)
        if (!asc) c = -c
        i += 1
      }
      c
    }
  }

  private def cmpCell(a: Any, b: Any): Int = {
    if (a == null && b == null) {
      0
    } else if (a == null) {
      -1
    } else if (b == null) {
      1
    } else {
      (ParquetIO.toDouble(a), ParquetIO.toDouble(b)) match {
        case (Some(x), Some(y)) => java.lang.Double.compare(x, y)
        case _ => a.toString.compareTo(b.toString)
      }
    }
  }

  private def aggTable(
      table: ParquetIO.Table,
      groups: Seq[String],
      aggs: Seq[AggCall]): VegamPage = {
    val gIdx = groups.map(table.colIndex)
    val state = new mutable.LinkedHashMap[String, AggState]()
    table.rows.foreach { row =>
      val k = if (gIdx.isEmpty) "" else keyOfNulls(row, gIdx)
      val st = state.getOrElseUpdate(k, new AggState(groups.length, aggs.length, row, gIdx))
      var i = 0
      while (i < aggs.length) {
        val a = aggs(i)
        val cell = if (a.col.isEmpty) null else {
          val idx = table.colIndex(a.col)
          if (idx >= 0) row(idx) else null
        }
        st.add(i, a.kind, cell)
        i += 1
      }
    }
    val n = state.size
    val cols = groups.length + aggs.length
    val values = new Array[Double](n * cols)
    val nulls = new Array[Boolean](n * cols)
    val texts = new Array[String](n * cols)
    val scales = Array.fill(cols)(0)
    var c = 0
    while (c < groups.length) {
      scales(c) = 0
      c += 1
    }
    while (c < cols) {
      scales(c) = aggs(c - groups.length).scale
      c += 1
    }
    var r = 0
    state.values.foreach { st =>
      var col = 0
      while (col < groups.length) {
        val cell = st.groupVals(col)
        writeCell(values, nulls, texts, r, col, cols, cell)
        col += 1
      }
      var ai = 0
      while (ai < aggs.length) {
        writeCell(values, nulls, texts, r, groups.length + ai, cols, st.result(ai, aggs(ai).kind))
        ai += 1
      }
      r += 1
    }
    VegamPage(n, cols, values, nulls, scales, texts)
  }

  private def toPage(table: ParquetIO.Table): VegamPage = {
    val n = table.rows.length
    val cols = table.names.length
    val values = new Array[Double](n * cols)
    val nulls = new Array[Boolean](n * cols)
    val texts = new Array[String](n * cols)
    var r = 0
    while (r < n) {
      var c = 0
      while (c < cols) {
        writeCell(values, nulls, texts, r, c, cols, table.rows(r)(c))
        c += 1
      }
      r += 1
    }
    VegamPage(n, cols, values, nulls, Array.fill(cols)(0), texts)
  }

  private def writeCell(
      values: Array[Double],
      nulls: Array[Boolean],
      texts: Array[String],
      row: Int,
      col: Int,
      numCols: Int,
      cell: Any): Unit = {
    val i = row * numCols + col
    if (cell == null) {
      nulls(i) = true
      values(i) = Double.NaN
    } else {
      ParquetIO.toDouble(cell) match {
        case Some(d) if !cell.isInstanceOf[String] =>
          values(i) = d
          nulls(i) = false
        case Some(d) =>
          values(i) = d
          nulls(i) = false
          texts(i) = cell.toString
        case None =>
          texts(i) = cell.toString
          nulls(i) = false
          values(i) = Double.NaN
      }
    }
  }

  private class AggState(nGroup: Int, nAgg: Int, first: Array[Any], gIdx: Seq[Int]) {
    val groupVals: Array[Any] = Array.tabulate(nGroup) { i =>
      val j = gIdx(i)
      if (j >= 0) first(j) else null
    }
    private val sums = new Array[Double](nAgg)
    private val mins = new Array[Double](nAgg)
    private val maxs = new Array[Double](nAgg)
    private val counts = new Array[Long](nAgg)
    private val has = new Array[Boolean](nAgg)

    def add(i: Int, kind: Int, cell: Any): Unit = {
      kind match {
        case NativePlan.AGG_COUNT_STAR =>
          counts(i) += 1
          has(i) = true
        case NativePlan.AGG_COUNT =>
          if (cell != null) {
            counts(i) += 1
            has(i) = true
          }
        case _ =>
          ParquetIO.toDouble(cell) match {
            case Some(d) =>
              if (!has(i)) {
                mins(i) = d
                maxs(i) = d
                has(i) = true
              } else {
                if (d < mins(i)) mins(i) = d
                if (d > maxs(i)) maxs(i) = d
              }
              sums(i) += d
              counts(i) += 1
            case None =>
          }
      }
    }

    def result(i: Int, kind: Int): Any = {
      if (!has(i) && kind != NativePlan.AGG_COUNT_STAR && kind != NativePlan.AGG_COUNT) {
        null
      } else {
        kind match {
          case NativePlan.AGG_SUM => sums(i)
          case NativePlan.AGG_COUNT | NativePlan.AGG_COUNT_STAR => counts(i).toDouble
          case NativePlan.AGG_MIN => mins(i)
          case NativePlan.AGG_MAX => maxs(i)
          case NativePlan.AGG_AVG => if (counts(i) == 0) null else sums(i) / counts(i).toDouble
          case _ => null
        }
      }
    }
  }
}
