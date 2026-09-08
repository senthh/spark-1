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

import org.apache.spark.sql.vegam.plan.NativePlan

trait VegamTask extends Serializable {
  def nextPage(): Option[VegamPage]
  def close(): Unit
}

/**
 * Columnar page produced by a backend. Values are doubles for the first cut;
 * null is encoded as NaN plus isNull.
 */
final class VegamPage(
    val numRows: Int,
    val numCols: Int,
    private val values: Array[Double],
    private val nulls: Array[Boolean],
    private val scales: Array[Int],
    private val texts: Array[String] = Array.empty) extends Serializable {

  def isNull(row: Int, col: Int): Boolean = nulls(row * numCols + col)

  def getDouble(row: Int, col: Int): Double = values(row * numCols + col)

  def getText(row: Int, col: Int): String = {
    val i = row * numCols + col
    if (texts != null && i < texts.length) texts(i) else null
  }

  def isText(row: Int, col: Int): Boolean = {
    val i = row * numCols + col
    texts != null && i < texts.length && texts(i) != null
  }

  def sumScale(col: Int): Int = if (col < scales.length) scales(col) else 0
}

object VegamPage {
  def apply(
      numRows: Int,
      numCols: Int,
      values: Array[Double],
      nulls: Array[Boolean],
      scales: Array[Int],
      texts: Array[String] = Array.empty): VegamPage = {
    new VegamPage(numRows, numCols, values, nulls, scales, texts)
  }
}

trait VegamBackend extends Serializable {
  def createTask(plan: NativePlan): VegamTask
}

object VegamBackend {
  val NATIVE: String = "native"
  val JVM: String = "jvm"
  val AUTO: String = "auto"

  def resolve(requested: String): Option[String] = requested match {
    case NATIVE if NativeTask.isLoaded => Some(NATIVE)
    case JVM => Some(JVM)
    case AUTO if NativeTask.isLoaded => Some(NATIVE)
    case AUTO => Some(JVM)
    case NATIVE => None
    case _ => None
  }

  def supports(backend: String, plan: NativePlan): Boolean = backend match {
    case JVM => true
    case NATIVE if NativeTask.isLoaded =>
      plan match {
        case _: org.apache.spark.sql.vegam.plan.CountStar => true
        case _: org.apache.spark.sql.vegam.plan.HashAgg => true
        case _: org.apache.spark.sql.vegam.plan.StagePlan => true
        case _ => false
      }
    case _ => false
  }

  def pick(requested: String, plan: NativePlan): Option[String] = {
    resolve(requested) match {
      case Some(NATIVE) if supports(NATIVE, plan) => Some(NATIVE)
      case Some(NATIVE) if requested == AUTO || requested.toLowerCase(java.util.Locale.ROOT) == AUTO =>
        Some(JVM)
      case Some(JVM) => Some(JVM)
      case other => other.filter(supports(_, plan))
    }
  }

  def create(name: String): VegamBackend = name match {
    case NATIVE => NativeBackend
    case JVM => JvmBackend
    case other =>
      throw new IllegalArgumentException(s"unknown vegam backend: $other")
  }
}
