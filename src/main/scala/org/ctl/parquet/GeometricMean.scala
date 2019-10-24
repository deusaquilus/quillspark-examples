package org.ctl.parquet

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class GeometricMean extends UserDefinedAggregateFunction {
  val MyDecimalType = DecimalType(38,18)

  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", MyDecimalType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("count", LongType) ::
      StructField("product", MyDecimalType) :: Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = MyDecimalType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = BigDecimal(1)
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Long](0) + 1
    buffer(1) = Option(buffer.getDecimal(1)).map(BigDecimal(_)).getOrElse(BigDecimal(1)) * BigDecimal(input.getDecimal(0))
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    buffer1(1) = BigDecimal(buffer1.getDecimal(1)) * BigDecimal(buffer2.getDecimal(1))
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    import spire.implicits._
    import spire.math._
    pow(BigDecimal(buffer.getDecimal(1)), BigDecimal(1)/buffer.getAs[Long](0))
  }
}
