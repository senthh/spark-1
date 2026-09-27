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

import org.apache.spark.sql.types.DataType

/**
 * Closed IR for one Spark stage. Serialized into the task and executed by a
 * Vegam backend. Exchange / shuffle is not represented.
 */
sealed trait NativePlan extends Serializable {
  def files: Seq[String]
  def withFiles(newFiles: Seq[String]): NativePlan
  /** Replaces the probe-side file ranges with one task's split. */
  def withProbeRefs(refs: Seq[FileRef]): NativePlan
}

object NativePlan {
  val FILTER_GT: Int = 1
  val FILTER_GTE: Int = 2
  val FILTER_LT: Int = 3
  val FILTER_LTE: Int = 4
  val FILTER_EQ: Int = 5
  val FILTER_NE: Int = 6

  val AGG_SUM: Int = 1
  val AGG_COUNT: Int = 2
  val AGG_COUNT_STAR: Int = 3
  val AGG_MIN: Int = 4
  val AGG_MAX: Int = 5
  val AGG_AVG: Int = 6

  val JOIN_INNER: Int = 1
  val JOIN_LEFT: Int = 2
  val JOIN_SEMI: Int = 3
  val JOIN_ANTI: Int = 4

  val WIN_ROW_NUMBER: Int = 1
  val WIN_RANK: Int = 2
  val WIN_DENSE_RANK: Int = 3
  val WIN_SUM: Int = 4
  val WIN_MIN: Int = 5
  val WIN_MAX: Int = 6

  // WindowCall.frame. PARTITION is the whole partition; RUNNING is
  // rows between unbounded preceding and current row.
  val FRAME_PARTITION: Int = 0
  val FRAME_RUNNING: Int = 1

  // AggCall.mode. UPDATE is a file-stage partial (raw column in, buffer out).
  val MODE_UPDATE: Int = 0
  val MODE_PARTIAL: Int = 1
  val MODE_MERGE: Int = 2
  val MODE_FINAL: Int = 3

  val EXPAND_COL: Int = 1
  val EXPAND_NULL: Int = 2
  val EXPAND_LONG: Int = 3
  val EXPAND_DOUBLE: Int = 4
  val EXPAND_STR: Int = 5

  // AggCall.input: how the aggregate input is derived from AggCall.col.
  val INPUT_UNSCALED: String = "unscaled"
  val INPUT_CAST_PREFIX: String = "cast:"
  val INPUT_UNSUPPORTED: String = "?"
}

case class FilterPred(
    col: String,
    value: Long,
    op: Int,
    strValue: String = "",
    dvalue: Double = Double.NaN,
    rightCol: String = "",
    orGroup: Int = 0) extends Serializable {
  def numValue: Double = if (dvalue.isNaN) value.toDouble else dvalue
  def isString: Boolean = strValue != null && strValue.nonEmpty
}

/**
 * `input` is empty when the aggregate reads `col` as is, [[NativePlan.INPUT_UNSCALED]]
 * for UnscaledValue(col), `cast:<type>` for Cast(col as type), and
 * [[NativePlan.INPUT_UNSUPPORTED]] for anything else (exact backends must reject it).
 */
case class AggCall(
    kind: Int,
    col: String,
    scale: Int,
    dataType: DataType,
    input: String = "",
    mode: Int = NativePlan.MODE_UPDATE,
    buffers: Int = 1) extends Serializable

/**
 * One file range to read. `length < 0` means the whole file. Spark may split a
 * large file into several ranges; only readers that honor ranges may be given
 * ranges with `start > 0` (see [[FileRef.wholeFiles]]).
 */
case class FileRef(
    path: String,
    parts: Seq[(String, String)],
    start: Long = 0L,
    length: Long = -1L) extends Serializable

object FileRef {
  /** Keeps one whole-file ref per path, for readers that ignore ranges. */
  def wholeFiles(refs: Seq[FileRef]): Seq[FileRef] =
    refs.filter(_.start == 0L).map(_.copy(length = -1L))
}

/**
 * `types` holds the Spark type of each entry of `columns` as
 * `DataType.simpleString` (e.g. "int", "decimal(7,2)", "string"); it is empty
 * for plans built before types were carried.
 */
case class ScanSpec(
    files: Seq[FileRef],
    columns: Seq[String],
    types: Seq[String] = Nil) extends Serializable {
  def paths: Seq[String] = files.map(_.path)
}

/**
 * `broadcast` is true when the build side came from a BroadcastHashJoin, so
 * every task may read the whole build side next to its own probe split.
 */
case class BuildJoin(
    scan: ScanSpec,
    probeKeys: Seq[String],
    buildKeys: Seq[String],
    joinType: Int,
    filters: Seq[FilterPred],
    broadcast: Boolean = true) extends Serializable

case class WindowCall(
    kind: Int,
    col: String,
    alias: String,
    frame: Int = NativePlan.FRAME_PARTITION) extends Serializable

/** A computed column. `expr` is a [[VExpr]] encoding. */
case class NamedExpr(name: String, expr: String) extends Serializable

case class WinSpec(
    partitionBy: Seq[String],
    orderBy: Seq[(String, Boolean)],
    fns: Seq[WindowCall]) extends Serializable

case class ExpandSlot(
    kind: Int,
    col: String = "",
    lvalue: Long = 0L,
    dvalue: Double = 0.0,
    svalue: String = "") extends Serializable

case class ExpandSpec(
    outCols: Seq[String],
    projections: Seq[Seq[ExpandSlot]]) extends Serializable

case class CountStar(files: Seq[String]) extends NativePlan {
  override def withFiles(newFiles: Seq[String]): NativePlan = copy(files = newFiles)

  override def withProbeRefs(refs: Seq[FileRef]): NativePlan =
    copy(files = FileRef.wholeFiles(refs).map(_.path))
}

case class HashAgg(
    files: Seq[String],
    groupCol: String,
    sumCol: String,
    filter: Option[FilterPred],
    sumScale: Int,
    groupType: DataType,
    sumType: DataType,
    complete: Boolean,
    groupCols: Seq[String] = Nil,
    aggs: Seq[AggCall] = Nil,
    filters: Seq[FilterPred] = Nil,
    fileRefs: Seq[FileRef] = Nil) extends NativePlan {
  override def withFiles(newFiles: Seq[String]): NativePlan = {
    val byPath = fileRefs.map(f => f.path -> f).toMap
    copy(
      files = newFiles,
      fileRefs = newFiles.map(p => byPath.getOrElse(p, FileRef(p, Nil))))
  }

  override def withProbeRefs(refs: Seq[FileRef]): NativePlan = {
    val whole = FileRef.wholeFiles(refs)
    copy(files = whole.map(_.path), fileRefs = whole)
  }

  def groups: Seq[String] = if (groupCols.nonEmpty) groupCols else Seq(groupCol)

  def aggCalls: Seq[AggCall] = {
    if (aggs.nonEmpty) {
      aggs
    } else {
      Seq(AggCall(NativePlan.AGG_SUM, sumCol, sumScale, sumType))
    }
  }

  def allFilters: Seq[FilterPred] = if (filters.nonEmpty) filters else filter.toSeq

  def refs: Seq[FileRef] = {
    if (fileRefs.nonEmpty) {
      fileRefs
    } else {
      files.map(FileRef(_, Nil))
    }
  }
}

/**
 * One fused stage: probe scan, zero or more broadcast hash joins, optional
 * window, optional multi-agg. Used for TPC-DS class coverage.
 */
case class StagePlan(
    probe: ScanSpec,
    builds: Seq[BuildJoin],
    probeFilters: Seq[FilterPred],
    groups: Seq[String],
    groupTypes: Seq[DataType],
    aggs: Seq[AggCall],
    window: Option[WinSpec],
    complete: Boolean,
    expand: Option[ExpandSpec] = None,
    projects: Seq[NamedExpr] = Nil,
    rowSource: Boolean = false,
    groupOrdinals: Seq[Int] = Nil,
    aggOrdinals: Seq[Seq[Int]] = Nil,
    resultAt: Seq[Int] = Nil,
    resultDiv: Seq[Int] = Nil,
    resultMul: Seq[Double] = Nil) extends NativePlan {

  /**
   * Plans the loaded native library cannot execute. Velox rejects them and
   * the library falls back to a kernel that collapses a group when any key
   * is null, drops every row when a filter column is missing, and holds whole
   * inputs in memory. With the auto backend those stages stay on Spark.
   */
  def jvmOnly: Boolean = {
    val probeCols = probe.columns.toSet
    // A measure that is not on the probe file is read from a join build.
    // The loaded library looks that name up on the probe and can drop it.
    val foreignAgg = aggs.exists { a =>
      val names = if (a.input.startsWith("(")) VExpr.colNames(a.input) else Seq(a.col)
      names.exists(n => n.nonEmpty && !probeCols.contains(n))
    }
    val dimFilter = probeFilters.exists(f => f.col.nonEmpty && !probeCols.contains(f.col))
    val semiJoin = builds.exists(b =>
      b.joinType == NativePlan.JOIN_SEMI || b.joinType == NativePlan.JOIN_ANTI)
    // Velox rejects these and the library falls back to its own kernel.
    val untyped = (probe +: builds.map(_.scan)).exists(s =>
      s.columns.isEmpty || s.types.length != s.columns.length)
    val veloxRejects = untyped || complete || (groups.isEmpty && aggs.isEmpty) ||
      builds.exists(!_.broadcast)
    veloxRejects || rowSource || resultAt.nonEmpty || projects.nonEmpty || foreignAgg ||
      expand.isDefined || window.isDefined || semiJoin || dimFilter ||
      probeFilters.exists(f => f.rightCol.nonEmpty || f.orGroup != 0) ||
      aggs.exists(a => a.input.startsWith("(")) ||
      builds.exists(b => (b.probeKeys ++ b.buildKeys).exists(_.startsWith("(")))
  }
  override def files: Seq[String] = probe.paths

  override def withFiles(newFiles: Seq[String]): NativePlan = {
    val byPath = probe.files.map(f => f.path -> f).toMap
    val next = newFiles.map(p => byPath.getOrElse(p, FileRef(p, Nil)))
    copy(probe = probe.copy(files = next))
  }

  override def withProbeRefs(refs: Seq[FileRef]): NativePlan =
    copy(probe = probe.copy(files = refs))
}
