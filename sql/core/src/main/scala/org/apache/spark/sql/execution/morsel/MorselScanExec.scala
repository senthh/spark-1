package org.apache.spark.sql.execution.morsel

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.LeafExecNode

case class MorselScanExec(
    filePath: String,
    output: Seq[Attribute],
    filterCol: Int = -1,
    filterValue: Long = 0)
  extends LeafExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    val columnNames = output.map(_.name).toArray
    
    val cleanPath = MorselPaths.clean(filePath)
    
    sparkContext.parallelize(Seq(cleanPath), 1).mapPartitions { iter =>
      if (!iter.hasNext) {
        Iterator.empty
      } else {
        val path = iter.next()
        val scheduler = MorselEngine.initScheduler(8)
        try {
          val batchHandle = MorselEngine.scanParquet(
            scheduler, path, columnNames, filterCol, filterValue)
          if (batchHandle == 0) {
            Iterator.empty
          } else {
            try {
              val numRows = MorselEngine.getBatchRows(batchHandle)
              (0 until numRows).iterator.map { _ => InternalRow.empty }
            } finally {
              MorselEngine.freeBatch(batchHandle)
            }
          }
        } finally {
          MorselEngine.shutdown(scheduler)
        }
      }
    }
  }
}
