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

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.fs.{FSDataInputStream, Path}

import org.apache.spark.sql.vegam.VegamPaths

/**
 * Hadoop FS byte source for libvegam. C++ (including Velox ReadFile) calls
 * these static methods instead of libhdfs. Short-circuit reads stay off
 * (see VegamPaths.hadoopConf).
 */
object HadoopBytes {
  private val streams = new ConcurrentHashMap[String, FSDataInputStream]()
  private val lengths = new ConcurrentHashMap[String, java.lang.Long]()

  def footerRowCount(path: String): Long = ParquetIO.footerRows(path)

  def load(
      path: String,
      columns: Array[String],
      partKeys: Array[String],
      partVals: Array[String]): NativeTable = {
    ParquetIO.loadNative(path, columns, partKeys, partVals)
  }

  /** File length in bytes. Used by Velox ReadFile::size. */
  def size(path: String): Long = {
    lengths.computeIfAbsent(path, p => {
      val hp = new Path(p)
      val fs = hp.getFileSystem(VegamPaths.hadoopConf())
      Long.box(fs.getFileStatus(hp).getLen)
    }).longValue()
  }

  /**
   * Positioned read of [offset, offset+length). The stream is cached per
   * path so Velox footer + column-chunk reads do not reopen Hadoop FS.
   */
  def pread(path: String, offset: Long, length: Int): Array[Byte] = {
    if (length <= 0) {
      return new Array[Byte](0)
    }
    val in = streams.computeIfAbsent(path, p => {
      val hp = new Path(p)
      val fs = hp.getFileSystem(VegamPaths.hadoopConf())
      fs.open(hp)
    })
    in.synchronized {
      in.seek(offset)
      val buf = new Array[Byte](length)
      in.readFully(buf, 0, length)
      buf
    }
  }

  def closePath(path: String): Unit = {
    val in = streams.remove(path)
    if (in != null) {
      in.close()
    }
    lengths.remove(path)
  }
}
