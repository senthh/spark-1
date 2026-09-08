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

  val EXPAND_COL: Int = 1
  val EXPAND_NULL: Int = 2
  val EXPAND_LONG: Int = 3
  val EXPAND_DOUBLE: Int = 4
  val EXPAND_STR: Int = 5
}

case class FilterPred(
    col: String,
    value: Long,
    op: Int,
    strValue: String = "",
    dvalue: Double = Double.NaN) extends Serializable {
  def numValue: Double = if (dvalue.isNaN) value.toDouble else dvalue
  def isString: Boolean = strValue != null && strValue.nonEmpty
}

case class AggCall(
    kind: Int,
    col: String,
    scale: Int,
    dataType: DataType) extends Serializable

case class FileRef(
    path: String,
    parts: Seq[(String, String)]) extends Serializable

case class ScanSpec(
    files: Seq[FileRef],
    columns: Seq[String]) extends Serializable {
  def paths: Seq[String] = files.map(_.path)
}

case class BuildJoin(
    scan: ScanSpec,
    probeKeys: Seq[String],
    buildKeys: Seq[String],
    joinType: Int,
    filters: Seq[FilterPred]) extends Serializable

case class WindowCall(kind: Int, col: String, alias: String) extends Serializable

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
    expand: Option[ExpandSpec] = None) extends NativePlan {
  override def files: Seq[String] = probe.paths

  override def withFiles(newFiles: Seq[String]): NativePlan = {
    val byPath = probe.files.map(f => f.path -> f).toMap
    val next = newFiles.map(p => byPath.getOrElse(p, FileRef(p, Nil)))
    copy(probe = probe.copy(files = next))
  }
}
