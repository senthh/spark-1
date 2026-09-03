# Changes to BaseSessionStateBuilder.scala

**File:** `sql/core/src/main/scala/org/apache/spark/sql/internal/BaseSessionStateBuilder.scala`

## 1. Add Import (after line 38)

```scala
import org.apache.spark.sql.execution.columnar.MorselColumnarRule
```

## 2. Modify columnarRules method (line 387)

**Original:**
```scala
protected def columnarRules: Seq[ColumnarRule] = {
  extensions.buildColumnarRules(session)
}
```

**Changed to:**
```scala
protected def columnarRules: Seq[ColumnarRule] = {
  MorselColumnarRule() +: extensions.buildColumnarRules(session)
}
```

This registers MorselColumnarRule to transform FileSourceScanExec → MorselScanExec.
