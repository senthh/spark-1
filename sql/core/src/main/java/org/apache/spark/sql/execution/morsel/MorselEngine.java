package org.apache.spark.sql.execution.morsel;

public class MorselEngine {
  
  static {
    // Load native library
    try {
      System.loadLibrary("morsel_engine");
    } catch (UnsatisfiedLinkError e) {
      System.err.println("Failed to load morsel_engine: " + e.getMessage());
    }
  }

  // Initialize scheduler
  public static native long initScheduler(int numThreads);

  // Scan parquet file
  public static native long scanParquet(
    long schedulerHandle,
    String filePath,
    String[] columns,
    int filterCol,
    long filterValue);

  // Get batch info
  public static native int getBatchRows(long batchHandle);
  public static native int getBatchCols(long batchHandle);

  // Free resources
  public static native void freeBatch(long batchHandle);
  public static native void shutdown(long schedulerHandle);
}
