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
   * Hadoop FS helpers used by C++ parquet open. Never copies to /tmp.
   * Short-circuit NativeIO is disabled; that path aborted YARN executors (134).
   */
  private static final ThreadLocal<HdfsHandle> HDFS = new ThreadLocal<>();

  private static final class HdfsHandle {
    String uri;
    FileSystem fs;
    FSDataInputStream in;
    long size;
    long pos;
  }

  private static Configuration readConf() {
    Configuration conf = new Configuration(SparkHadoopUtil.get().conf());
    conf.setBoolean("dfs.client.read.shortcircuit", false);
    conf.setBoolean("dfs.client.use.legacy.blockreader", false);
    conf.set("dfs.domain.socket.path", "");
    return conf;
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
    FileSystem fs = p.getFileSystem(readConf());
    HdfsHandle n = new HdfsHandle();
    n.uri = uri;
    n.fs = fs;
    n.in = fs.open(p);
    n.size = fs.getFileStatus(p).getLen();
    n.pos = 0L;
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
    try {
      return preadOnce(uri, offset, buf, n);
    } catch (Throwable t) {
      System.err.println("nativesql: hdfsPread retry uri=" + uri + " off=" + offset +
          " n=" + n + " err=" + t);
      t.printStackTrace(System.err);
      HdfsHandle old = HDFS.get();
      closeQuiet(old);
      HDFS.remove();
      return preadOnce(uri, offset, buf, n);
    }
  }

  private static int preadOnce(String uri, long offset, byte[] buf, int n) throws IOException {
    HdfsHandle h = openHdfs(uri);
    if (offset >= h.size || n <= 0) {
      return 0;
    }
    int want = n;
    if (offset + (long) want > h.size) {
      want = (int) (h.size - offset);
    }
    if (want > buf.length) {
      want = buf.length;
    }
    /* seek+readFully. Positioned/short-circuit reads abort on this cluster.
     * C++ only requests the parquet footer and surviving column chunks. */
    synchronized (h) {
      if (h.pos != offset) {
        try {
          h.in.seek(offset);
        } catch (IOException seekErr) {
          closeQuiet(h);
          HDFS.remove();
          h = openHdfs(uri);
          h.in.seek(offset);
        }
        h.pos = offset;
      }
      h.in.readFully(buf, 0, want);
      h.pos += want;
    }
    return want;
  }
}
