package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarRule, FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.morsel.MorselScanExec
import org.apache.spark.sql.internal.SQLConf

/**
 * Columnar rule to redirect FileSourceScanExec to MorselScanExec
 * when spark.sql.morsel.enabled is true
 */
class MorselColumnarRule extends ColumnarRule {
  
  override def preColumnarTransitions: Rule[SparkPlan] = {
    new Rule[SparkPlan] {
      override def apply(plan: SparkPlan): SparkPlan = {
        if (!SQLConf.get.getConfString("spark.sql.morsel.enabled", "false").toBoolean) {
          return plan
        }
        
        plan transformDown {
          case scan: FileSourceScanExec =>
            val paths = scan.relation.location.rootPaths
            if (paths.isEmpty) {
              scan
            } else {
              val filePath = paths.head.toString
              MorselScanExec(
                filePath = filePath,
                output = scan.output,
                filterCol = -1,
                filterValue = 0
              )
            }
        }
      }
    }
  }
}

object MorselColumnarRule {
  def apply(): MorselColumnarRule = new MorselColumnarRule()
}
