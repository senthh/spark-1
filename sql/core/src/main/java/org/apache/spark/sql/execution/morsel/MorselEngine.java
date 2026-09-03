package org.apache.spark.sql.execution.morsel;

public class MorselEngine {

  static {
    try {
      System.loadLibrary("morsel_engine");
    } catch (UnsatisfiedLinkError e) {
      System.err.println("Failed to load morsel_engine: " + e.getMessage());
    }
  }

  public static native long initScheduler(int numThreads);

  public static native long scanParquet(
    long schedulerHandle,
    String filePath,
    String[] columns,
    int filterCol,
    long filterValue);

  public static native long getBatchRows(long batchHandle);
  public static native int getBatchCols(long batchHandle);
  public static native void freeBatch(long batchHandle);
  public static native void shutdown(long schedulerHandle);

  public static native long footerRowCount(String filePath);

  public static native long hashAggregate(
    long schedulerHandle,
    String filePath,
    String groupCol,
    String sumCol,
    String filterCol,
    long filterValue);

  public static native int getAggRows(long handle);
  public static native void copyAggKeys(long handle, long[] keys);
  public static native void copyAggSums(long handle, double[] sums);
  public static native void freeAgg(long handle);
}
