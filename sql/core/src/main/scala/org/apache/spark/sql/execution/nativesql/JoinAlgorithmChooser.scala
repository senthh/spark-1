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

import org.apache.spark.sql.internal.SQLConf

/** ClickHouse-style native join algorithm tag. First match in [[JoinAlgorithmChooser]] wins. */
sealed trait NativeJoinAlgo

object NativeJoinAlgo {
  case object Lookup extends NativeJoinAlgo
  case object BroadcastHash extends NativeJoinAlgo
  case object SortMerge extends NativeJoinAlgo
  case object GraceHash extends NativeJoinAlgo
  case object PartitionedHash extends NativeJoinAlgo
}

/**
 * Picks a [[NativeJoinAlgo]] from estimated sizes and sort properties.
 * `rightLooksLikeIndex` is false unless the caller (or a conf override) sets it.
 */
object JoinAlgorithmChooser {

  def choose(
      rightLooksLikeIndex: Boolean = false,
      rightEstimatedBytes: Option[Long] = None,
      bothSidesSorted: Boolean = false): NativeJoinAlgo = {
    val conf = SQLConf.get
    if (rightLooksLikeIndex) {
      NativeJoinAlgo.Lookup
    } else if (rightEstimatedBytes.exists(_ <= conf.autoBroadcastJoinThreshold)) {
      NativeJoinAlgo.BroadcastHash
    } else if (bothSidesSorted) {
      NativeJoinAlgo.SortMerge
    } else if (rightEstimatedBytes.exists(_ > conf.nativeSqlJoinGraceThreshold)) {
      NativeJoinAlgo.GraceHash
    } else {
      NativeJoinAlgo.PartitionedHash
    }
  }
}
