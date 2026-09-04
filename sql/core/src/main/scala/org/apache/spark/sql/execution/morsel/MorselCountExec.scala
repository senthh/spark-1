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

package org.apache.spark.sql.execution.morsel

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.LeafExecNode

/** COUNT(*) from parquet footer metadata. Does not decode columns. */
case class MorselCountExec(filePaths: Seq[String], output: Seq[Attribute])
  extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val paths = filePaths.map(MorselPaths.clean)
    val schema = output
    sparkContext.parallelize(Seq(paths), 1).mapPartitions { iter =>
      if (!iter.hasNext) {
        Iterator.empty
      } else {
        val files = iter.next()
        var total = 0L
        files.foreach { p =>
          val n = MorselEngine.footerRowCount(p)
          if (n < 0) {
            throw SparkException.internalError(
              s"morsel footerRowCount failed for $p")
          }
          total += n
        }
        val proj = UnsafeProjection.create(schema.map(_.dataType).toArray)
        Iterator.single(proj(new GenericInternalRow(Array[Any](total))).copy())
      }
    }
  }
}
