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
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.vegam.plan.{
  AggCall, BuildJoin, CountStar, ExpandSlot, ExpandSpec, FileRef, HashAgg, NativePlan,
  StagePlan, VExpr, WinSpec}

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

    override def isDone: Boolean = done

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

  /**
   * Aggregate or window over rows already produced by Spark (a shuffle).
   * `rows` are positional against `plan.probe.columns`.
   */
  def rowStage(plan: StagePlan, rows: Seq[Array[Any]]): VegamPage = {
    if (plan.window.isDefined && plan.aggs.isEmpty) {
      val names = plan.probe.columns.toArray
      val table = new ParquetIO.Table(names, ArrayBuffer.from(rows))
      toPage(window(table, plan.window.get))
    } else {
      mergeRows(plan, rows)
    }
  }

  private def stage(s: StagePlan): VegamPage = {
    // Only columns that exist on the probe file. A measure that lives on
    // the build must not be requested here: a missing parquet field is a
    // null column of that name, and the join then drops the real one.
    val probeNames = s.probe.columns.toSet
    def onProbe(c: String): Boolean = c.nonEmpty && probeNames.contains(c)
    val probeWant = (
      s.probe.columns ++
        s.aggs.map(_.col).filter(onProbe) ++
        s.aggs.flatMap(a => VExpr.colNames(a.input)).filter(onProbe) ++
        s.projects.flatMap(p => (p.name +: VExpr.colNames(p.expr)).filter(onProbe)) ++
        s.builds.flatMap(_.probeKeys).flatMap(keyCols).filter(onProbe)
      ).distinct
    // Residual FilterExec preds (dim columns) must not run on the fact
    // scan. Same rule as vegam_run_decoded: filter after joins.
    var table = readAll(s.probe.files, probeWant, Nil)
    s.builds.foreach { b =>
      table = join(table, b)
    }
    if (s.projects.nonEmpty) {
      table = projectTable(table, s.projects)
    }
    if (s.probeFilters.nonEmpty) {
      table = filterTable(table, s.probeFilters)
    }
    s.expand.foreach { e =>
      table = expand(table, e)
    }
    s.window.foreach { w =>
      table = window(table, w)
    }
    val page = if (s.aggs.nonEmpty || s.groups.nonEmpty) {
      aggTable(table, s.groups, s.aggs)
    } else {
      toPage(table)
    }
    relayout(page, s.resultAt, s.resultDiv, s.resultMul)
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

  private def filterTable(
      table: ParquetIO.Table,
      filters: Seq[org.apache.spark.sql.vegam.plan.FilterPred]): ParquetIO.Table = {
    if (filters.isEmpty) {
      table
    } else {
      val kept = table.rows.filter(r => ParquetIO.keep(r, table.names, filters))
      new ParquetIO.Table(table.names, kept)
    }
  }

  private def join(probe: ParquetIO.Table, b: BuildJoin): ParquetIO.Table = {
    val buildWant = (b.buildKeys ++ b.filters.map(_.col) ++ b.scan.columns)
      .filter(_.nonEmpty).distinct
    val build = readAll(b.scan.files, buildWant, b.filters)
    val exprKeys = (b.probeKeys ++ b.buildKeys).exists(_.startsWith("("))
    val pIdx = b.probeKeys.map(probe.colIndex)
    val bIdx = b.buildKeys.map(build.colIndex)
    val index = new mutable.HashMap[String, ArrayBuffer[Array[Any]]]()
    build.rows.foreach { row =>
      val k = if (exprKeys) joinKey(b.buildKeys, row, build.names) else keyOf(row, bIdx)
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
      val k = if (exprKeys) joinKey(b.probeKeys, prow, probe.names) else keyOf(prow, pIdx)
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

  private def keyCols(key: String): Seq[String] = {
    if (key.startsWith("(")) VExpr.colNames(key) else Seq(key)
  }

  private def joinKey(keys: Seq[String], row: Array[Any], names: Array[String]): String = {
    val b = new StringBuilder
    var i = 0
    while (i < keys.length) {
      val k = keys(i)
      val v = if (k.startsWith("(")) {
        VExpr.evalEncoded(k, names, row)
      } else {
        val idx = names.indexOf(k)
        if (idx < 0) null else row(idx)
      }
      if (v == null) {
        return null
      }
      if (i > 0) b.append('\u0001')
      b.append(cellKey(v))
      i += 1
    }
    b.toString
  }

  private def cellKey(v: Any): String = v match {
    case d: java.lang.Double if d == d.longValue().toDouble => d.longValue().toString
    case d: Double if d == d.toLong.toDouble => d.toLong.toString
    case b: java.lang.Boolean => b.toString
    case other => other.toString
  }

  private def projectTable(
      table: ParquetIO.Table,
      projects: Seq[org.apache.spark.sql.vegam.plan.NamedExpr]): ParquetIO.Table = {
    val grown = ArrayBuffer.empty[String]
    grown ++= table.names
    val plan = projects.map { p =>
      val i = grown.indexOf(p.name)
      if (i >= 0) {
        (i, p.expr)
      } else {
        val at = grown.length
        grown += p.name
        (at, p.expr)
      }
    }
    val width = grown.length
    val nameArr = grown.toArray
    val out = new ArrayBuffer[Array[Any]]()
    table.rows.foreach { row =>
      val cur = Array.ofDim[Any](width)
      Array.copy(row, 0, cur, 0, row.length)
      plan.foreach { case (i, expr) =>
        cur(i) = VExpr.evalEncoded(expr, nameArr, cur)
      }
      out += cur
    }
    new ParquetIO.Table(nameArr, out)
  }

  private def outWidth(a: AggCall): Int = {
    if (a.mode == NativePlan.MODE_FINAL) 1 else math.max(a.buffers, 1)
  }

  private def outCells(st: AggState, a: AggCall, i: Int): Seq[Any] = {
    if (a.mode != NativePlan.MODE_FINAL && a.buffers >= 2 &&
        a.kind == NativePlan.AGG_AVG) {
      val sum = if (st.seen(i)) st.sumAt(i) else 0.0
      Seq(sum, st.countAt(i).toDouble)
    } else if (a.mode != NativePlan.MODE_FINAL && a.buffers >= 2 &&
        a.kind == NativePlan.AGG_SUM) {
      if (st.seen(i)) Seq(st.sumAt(i), 0.0) else Seq(0.0, 1.0)
    } else {
      Seq(st.result(i, a.kind))
    }
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
    // A null in one partition column must not collapse every such row
    // into a single group. keyOf does that; keyOfNulls keeps the rest.
    val groups = table.rows.groupBy(r => keyOfNulls(r, partIdx)).values
    val extraNames = w.fns.map(_.alias).toArray
    val names = table.names ++ extraNames
    val out = new ArrayBuffer[Array[Any]]()
    groups.foreach { buf =>
      val sorted = buf.sortInPlace()(ord(orderIdx))
      val partVal = w.fns.map { f =>
        if (f.frame == NativePlan.FRAME_RUNNING) {
          null
        } else {
          partAgg(sorted, table.colIndex(f.col), f.kind)
        }
      }
      val run = Array.fill(w.fns.length)(null: Any)
      var i = 0
      var prevKey: String = null
      var rank = 0
      var dense = 0
      while (i < sorted.length) {
        val row = sorted(i)
        val ok = keyOfNulls(row, orderIdx.map(_._1))
        if (i == 0 || ok != prevKey) {
          rank = i + 1
          dense += 1
          prevKey = ok
        }
        val extra = w.fns.zipWithIndex.map { case (f, fi) =>
          val ci = if (f.col.isEmpty) -1 else table.colIndex(f.col)
          val cell = if (ci >= 0) row(ci) else null
          f.kind match {
            case NativePlan.WIN_ROW_NUMBER => (i + 1).toLong
            case NativePlan.WIN_RANK => rank.toLong
            case NativePlan.WIN_DENSE_RANK => dense.toLong
            case NativePlan.WIN_SUM if f.frame == NativePlan.FRAME_RUNNING =>
              run(fi) = addNum(run(fi), cell)
              run(fi)
            case NativePlan.WIN_MIN if f.frame == NativePlan.FRAME_RUNNING =>
              run(fi) = extreme(run(fi), cell, less = true)
              run(fi)
            case NativePlan.WIN_MAX if f.frame == NativePlan.FRAME_RUNNING =>
              run(fi) = extreme(run(fi), cell, less = false)
              run(fi)
            case _ => partVal(fi)
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

  private def partAgg(rows: ArrayBuffer[Array[Any]], idx: Int, kind: Int): Any = {
    var acc: Any = null
    rows.foreach { r =>
      val cell = if (idx >= 0 && idx < r.length) r(idx) else null
      kind match {
        case NativePlan.WIN_SUM => acc = addNum(acc, cell)
        case NativePlan.WIN_MIN => acc = extreme(acc, cell, less = true)
        case NativePlan.WIN_MAX => acc = extreme(acc, cell, less = false)
        case _ =>
      }
    }
    acc
  }

  private def addNum(prev: Any, cell: Any): Any = {
    ParquetIO.toDouble(cell) match {
      case Some(d) =>
        val p = if (prev == null) 0.0 else prev.asInstanceOf[Double]
        p + d
      case None => prev
    }
  }

  private def extreme(prev: Any, cell: Any, less: Boolean): Any = {
    ParquetIO.toDouble(cell) match {
      case Some(d) =>
        if (prev == null) {
          d
        } else {
          val p = prev.asInstanceOf[Double]
          if (less && d < p) d else if (!less && d > p) d else p
        }
      case None => prev
    }
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
        val cell = if (a.input.startsWith("(")) {
          VExpr.evalEncoded(a.input, table.names, row)
        } else if (a.col.isEmpty) {
          null
        } else {
          val idx = table.colIndex(a.col)
          if (idx >= 0) row(idx) else null
        }
        st.add(i, a.kind, cell)
        i += 1
      }
    }
    val n = state.size
    val widths = aggs.map(outWidth)
    val cols = groups.length + widths.sum
    val values = new Array[Double](n * cols)
    val nulls = new Array[Boolean](n * cols)
    val texts = new Array[String](n * cols)
    val scales = Array.fill(cols)(0)
    var c = 0
    while (c < groups.length) {
      scales(c) = 0
      c += 1
    }
    var ai = 0
    while (ai < aggs.length) {
      var k = 0
      while (k < widths(ai)) {
        scales(c) = if (k == 0) aggs(ai).scale else 0
        c += 1
        k += 1
      }
      ai += 1
    }
    var r = 0
    state.values.foreach { st =>
      var col = 0
      while (col < groups.length) {
        val cell = st.groupVals(col)
        writeCell(values, nulls, texts, r, col, cols, cell)
        col += 1
      }
      ai = 0
      while (ai < aggs.length) {
        val cells = outCells(st, aggs(ai), ai)
        var k = 0
        while (k < cells.length) {
          writeCell(values, nulls, texts, r, col, cols, cells(k))
          col += 1
          k += 1
        }
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

  private def mergeRows(plan: StagePlan, rows: Seq[Array[Any]]): VegamPage = {
    val gOrds = plan.groupOrdinals
    val calls = plan.aggs
    val ords = plan.aggOrdinals
    val state = new mutable.LinkedHashMap[String, (Array[Any], Array[Array[Any]])]()
    rows.foreach { row =>
      val k = if (gOrds.isEmpty) "" else gOrds.map { i =>
        if (i < 0 || i >= row.length || row(i) == null) "\u0000" else cellKey(row(i))
      }.mkString("\u0001")
      val (gs, bufs) = state.getOrElseUpdate(k, {
        val g = gOrds.map(i => if (i >= 0 && i < row.length) row(i) else null).toArray
        (g, calls.map(initBuf).toArray)
      })
      var i = 0
      while (i < calls.length) {
        val ins = if (i < ords.length) ords(i) else Nil
        absorb(bufs(i), calls(i), ins, row, plan.probe.columns.toArray)
        i += 1
      }
      state.update(k, (gs, bufs))
    }
    val finished = calls.nonEmpty && calls.forall(_.mode == NativePlan.MODE_FINAL)
    val entries = if (state.isEmpty && gOrds.isEmpty && finished) {
      Seq((Array.empty[Any], calls.map(initBuf).toArray))
    } else {
      state.values.toSeq
    }
    val n = entries.length
    val spec = if (plan.resultAt.isEmpty) {
      val w = gOrds.length + calls.map(outWidth).sum
      (0 until w).map(i => (i, 0, 1.0))
    } else {
      val muls = if (plan.resultMul.isEmpty) {
        Seq.fill(plan.resultAt.length)(1.0)
      } else {
        plan.resultMul
      }
      plan.resultAt.zipWithIndex.map { case (src, i) =>
        val div = if (i < plan.resultDiv.length) plan.resultDiv(i) else 0
        val mul = if (i < muls.length) muls(i) else 1.0
        (src, div, mul)
      }
    }
    val cols = math.max(spec.length, 1)
    val values = new Array[Double](math.max(n, 0) * cols)
    val nulls = new Array[Boolean](math.max(n, 0) * cols)
    val texts = new Array[String](math.max(n, 0) * cols)
    val scales = Array.fill(cols)(0)
    var r = 0
    entries.foreach { case (gs, bufs) =>
      val raw = new ArrayBuffer[Any]()
      raw ++= gs
      var ai = 0
      while (ai < calls.length) {
        raw ++= finishBuf(bufs(ai), calls(ai))
        ai += 1
      }
      var c = 0
      spec.foreach { case (src, div, mul) =>
        val cell = if (src >= 0 && src < raw.length) raw(src) else null
        writeCell(values, nulls, texts, r, c, cols, scaleMul(divideScale(cell, div), mul))
        c += 1
      }
      r += 1
    }
    VegamPage(n, spec.length, values, nulls, scales, texts)
  }

  /** Reorder a groups-then-aggs page into the aggregate's result list. */
  private def relayout(
      page: VegamPage,
      at: Seq[Int],
      div: Seq[Int],
      mul: Seq[Double]): VegamPage = {
    if (at.isEmpty) {
      return page
    }
    val n = page.numRows
    val cols = at.length
    val values = new Array[Double](math.max(n, 0) * math.max(cols, 1))
    val nulls = new Array[Boolean](math.max(n, 0) * math.max(cols, 1))
    val texts = new Array[String](math.max(n, 0) * math.max(cols, 1))
    var r = 0
    while (r < n) {
      var c = 0
      while (c < cols) {
        val src = at(c)
        val d = if (c < div.length) div(c) else 0
        val m = if (c < mul.length) mul(c) else 1.0
        val cell = if (src < 0 || src >= page.numCols || page.isNull(r, src)) {
          null
        } else if (page.isText(r, src)) {
          page.getText(r, src)
        } else {
          scaleMul(divideScale(page.getDouble(r, src), d), m)
        }
        writeCell(values, nulls, texts, r, c, cols, cell)
        c += 1
      }
      r += 1
    }
    VegamPage(n, cols, values, nulls, Array.fill(math.max(cols, 1))(0), texts)
  }

  /** Apply a result-list literal factor. 1.0 leaves the cell unchanged. */
  private def scaleMul(cell: Any, factor: Double): Any = {
    if (cell == null || factor == 1.0) {
      cell
    } else {
      val f = BigDecimal(factor)
      cell match {
        case n: java.lang.Long => BigDecimal(n.longValue()) * f
        case n: java.lang.Integer => BigDecimal(n.intValue()) * f
        case n: java.lang.Double => n.doubleValue() * factor
        case n: Double => n * factor
        case n: Decimal => n.toBigDecimal * f
        case n: BigDecimal => n * f
        case _ =>
          ParquetIO.toDouble(cell).map(_ * factor).orNull
      }
    }
  }

  /** Unscaled long to decimal: divide by 10^scale. scale 0 leaves the cell. */
  private def divideScale(cell: Any, scale: Int): Any = {
    if (cell == null || scale <= 0) {
      cell
    } else {
      val unscaled = cell match {
        case n: java.lang.Long => BigDecimal(n.longValue())
        case n: java.lang.Integer => BigDecimal(n.intValue())
        case n: java.lang.Double => BigDecimal(n.doubleValue())
        case n: Decimal => n.toBigDecimal
        case _ =>
          ParquetIO.toDouble(cell).map(d => BigDecimal(d)).orNull
      }
      if (unscaled == null) null
      else unscaled / BigDecimal(10).pow(scale)
    }
  }

  private def initBuf(a: AggCall): Array[Any] = {
    a.kind match {
      case NativePlan.AGG_AVG => Array(null, Long.box(0L))
      case NativePlan.AGG_SUM if a.buffers >= 2 => Array(null, java.lang.Boolean.TRUE)
      case NativePlan.AGG_COUNT | NativePlan.AGG_COUNT_STAR => Array(Long.box(0L))
      case _ => Array(null)
    }
  }

  private def absorb(
      buf: Array[Any],
      a: AggCall,
      ins: Seq[Int],
      row: Array[Any],
      names: Array[String]): Unit = {
    val merging = a.mode == NativePlan.MODE_FINAL || a.mode == NativePlan.MODE_MERGE
    if (merging) {
      absorbMerge(buf, a, ins, row)
    } else {
      val cell = if (a.input.startsWith("(")) {
        VExpr.evalEncoded(a.input, names, row)
      } else if (ins.nonEmpty && ins.head >= 0 && ins.head < row.length) {
        row(ins.head)
      } else {
        null
      }
      absorbRaw(buf, a, cell)
    }
  }

  private def absorbRaw(buf: Array[Any], a: AggCall, cell: Any): Unit = {
    a.kind match {
      case NativePlan.AGG_COUNT_STAR =>
        buf(0) = Long.box(buf(0).asInstanceOf[Long] + 1L)
      case NativePlan.AGG_COUNT =>
        if (cell != null) buf(0) = Long.box(buf(0).asInstanceOf[Long] + 1L)
      case NativePlan.AGG_AVG =>
        ParquetIO.toDouble(cell).foreach { d =>
          val s = if (buf(0) == null) 0.0 else buf(0).asInstanceOf[Double]
          buf(0) = s + d
          buf(1) = Long.box(buf(1).asInstanceOf[Long] + 1L)
        }
      case NativePlan.AGG_MIN =>
        buf(0) = extreme(buf(0), cell, less = true)
      case NativePlan.AGG_MAX =>
        buf(0) = extreme(buf(0), cell, less = false)
      case _ =>
        ParquetIO.toDouble(cell).foreach { d =>
          val s = if (buf(0) == null) 0.0 else buf(0).asInstanceOf[Double]
          buf(0) = s + d
          if (buf.length > 1) buf(1) = java.lang.Boolean.FALSE
        }
    }
  }

  private def absorbMerge(buf: Array[Any], a: AggCall, ins: Seq[Int], row: Array[Any]): Unit = {
    def at(j: Int): Any = {
      if (j < ins.length && ins(j) >= 0 && ins(j) < row.length) row(ins(j)) else null
    }
    a.kind match {
      case NativePlan.AGG_COUNT | NativePlan.AGG_COUNT_STAR =>
        val n = ParquetIO.toLong(at(0)).getOrElse(0L)
        buf(0) = Long.box(buf(0).asInstanceOf[Long] + n)
      case NativePlan.AGG_AVG =>
        val cnt = ParquetIO.toLong(at(1)).getOrElse(0L)
        if (cnt != 0L) {
          val s = if (buf(0) == null) 0.0 else buf(0).asInstanceOf[Double]
          ParquetIO.toDouble(at(0)).foreach(d => buf(0) = s + d)
          buf(1) = Long.box(buf(1).asInstanceOf[Long] + cnt)
        }
      case NativePlan.AGG_SUM if a.buffers >= 2 =>
        val empty = at(1) == true || at(1) == java.lang.Boolean.TRUE
        if (!empty) {
          ParquetIO.toDouble(at(0)).foreach { d =>
            val s = if (buf(0) == null) 0.0 else buf(0).asInstanceOf[Double]
            buf(0) = s + d
            buf(1) = java.lang.Boolean.FALSE
          }
        }
      case NativePlan.AGG_MIN =>
        buf(0) = extreme(buf(0), at(0), less = true)
      case NativePlan.AGG_MAX =>
        buf(0) = extreme(buf(0), at(0), less = false)
      case _ =>
        ParquetIO.toDouble(at(0)).foreach { d =>
          val s = if (buf(0) == null) 0.0 else buf(0).asInstanceOf[Double]
          buf(0) = s + d
        }
    }
  }

  private def finishBuf(buf: Array[Any], a: AggCall): Seq[Any] = {
    if (a.mode == NativePlan.MODE_FINAL) {
      a.kind match {
        case NativePlan.AGG_AVG =>
          val c = buf(1).asInstanceOf[Long]
          if (c == 0L) Seq(null) else Seq(buf(0).asInstanceOf[Double] / c.toDouble)
        case NativePlan.AGG_SUM if a.buffers >= 2 =>
          if (buf(1) == java.lang.Boolean.TRUE) Seq(null) else Seq(buf(0))
        case NativePlan.AGG_COUNT | NativePlan.AGG_COUNT_STAR =>
          Seq(buf(0).asInstanceOf[Long].toDouble)
        case _ =>
          Seq(buf(0))
      }
    } else {
      a.kind match {
        case NativePlan.AGG_AVG =>
          Seq(if (buf(0) == null) 0.0 else buf(0), buf(1).asInstanceOf[Long].toDouble)
        case NativePlan.AGG_SUM if buf.length > 1 =>
          val empty = buf(1) == java.lang.Boolean.TRUE
          if (empty) Seq(0.0, 1.0) else Seq(buf(0), 0.0)
        case NativePlan.AGG_COUNT | NativePlan.AGG_COUNT_STAR =>
          Seq(buf(0).asInstanceOf[Long].toDouble)
        case _ =>
          Seq(buf(0))
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

    def sumAt(i: Int): Double = sums(i)
    def countAt(i: Int): Long = counts(i)
    def seen(i: Int): Boolean = has(i)

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
