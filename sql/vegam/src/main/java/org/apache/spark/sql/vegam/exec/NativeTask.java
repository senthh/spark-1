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

package org.apache.spark.sql.vegam.exec;

/**
 * The only JNI surface. One task, not one method per operator.
 */
public final class NativeTask {

  private static final boolean LOADED;

  static {
    boolean ok = false;
    try {
      System.loadLibrary("vegam");
      ok = true;
    } catch (UnsatisfiedLinkError e) {
      System.err.println("vegam: libvegam not loaded: " + e.getMessage());
    }
    LOADED = ok;
  }

  private NativeTask() {
  }

  public static boolean isLoaded() {
    return LOADED;
  }

  /**
   * @return task handle, or 0 on failure
   */
  public static native long createTask(byte[] planBytes, long memoryBudget, int numThreads);

  /**
   * Fills row-major doubles and a null bitmap. Returns row count, or -1 at EOF,
   * or -2 on error.
   */
  public static native int nextPage(
      long handle,
      double[] values,
      boolean[] nulls,
      int[] meta);

  public static native void close(long handle);
}
