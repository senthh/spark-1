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

package org.apache.spark.sql.execution.nativesql;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.spark.deploy.SparkHadoopUtil;

/**
 * JNI surface for {@code libspark_nativesql_jni}.
 *
 * <p>Each input is a columnar batch: {@code Object[]} of primitive arrays
 * ({@code int[]}, {@code long[]}, {@code double[]}, {@code boolean[]}).
 *
 * <p>Result is {@code Object[]{ Integer numRows, Object[] columns }}.
 */
public final class NativeSqlJni {
  private NativeSqlJni() {}

  public static native Object[] execute(String planIr, Object[][] columns, int[] numRows);

  /**
   * Same as {@link #execute} but file leaves are native parquet scans.
   * {@code scans[i]} is {@code Object[]{String[] paths, long[] starts, long[] lengths,
   * Object[] blobs, String[] colNames, int[] colTypes}} or null.
   * Result is {@code Object[]{numRows, cols, long[] strHashes, byte[][] strValues}}.
   */
  public static native Object[] executeScan(
      String planIr, Object[][] columns, int[] numRows, Object[] scans);

  /**
   * Hadoop FS helpers used by C++ parquet open (no full-file copy to /tmp).
   * One cached stream per thread.
   */
  private static final ThreadLocal<HdfsHandle> HDFS = new ThreadLocal<>();

  private static final class HdfsHandle {
    String uri;
    FileSystem fs;
    FSDataInputStream in;
    long size;
  }

  private static HdfsHandle openHdfs(String uri) throws IOException {
    HdfsHandle h = HDFS.get();
    if (h != null && uri.equals(h.uri) && h.in != null) {
      return h;
    }
    if (h != null) {
      closeQuiet(h);
    }
    Path p = new Path(uri);
    Configuration conf = SparkHadoopUtil.get().conf();
    FileSystem fs = p.getFileSystem(conf);
    HdfsHandle n = new HdfsHandle();
    n.uri = uri;
    n.fs = fs;
    n.in = fs.open(p);
    n.size = fs.getFileStatus(p).getLen();
    HDFS.set(n);
    return n;
  }

  private static void closeQuiet(HdfsHandle h) {
    if (h == null) {
      return;
    }
    try {
      if (h.in != null) {
        h.in.close();
      }
    } catch (IOException ignored) {
    }
    h.in = null;
    h.uri = null;
  }

  public static long hdfsSize(String uri) throws IOException {
    return openHdfs(uri).size;
  }

  public static int hdfsPread(String uri, long offset, byte[] buf, int n) throws IOException {
    HdfsHandle h = openHdfs(uri);
    if (offset >= h.size || n <= 0) {
      return 0;
    }
    int want = n;
    if (offset + want > h.size) {
      want = (int) (h.size - offset);
    }
    if (want > buf.length) {
      want = buf.length;
    }
    /* Fresh stream per read: cached seek+read was throwing on fact files. */
    Path p = new Path(uri);
    try (FSDataInputStream in = h.fs.open(p)) {
      int got = in.read(offset, buf, 0, want);
      if (got < 0) {
        return 0;
      }
      int pos = got;
      while (pos < want) {
        int r = in.read(offset + pos, buf, pos, want - pos);
        if (r < 0) {
          break;
        }
        pos += r;
      }
      return pos;
    }
  }
}
