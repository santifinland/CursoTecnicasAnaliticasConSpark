/* Spark jobs */

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._

object Jobs {
  def main(args: Array[String]) = {

    // Create Spark Session
    val spark: SparkSession = SparkSession.builder.appName("Spark Course").getOrCreate()

    // Read data injecting schema
    val schema: StructType = StructType(
      StructField("CALLER", StringType, false) ::
      StructField("CALLED", StringType, false) ::
      StructField("DATE", DateType, true) ::
      StructField("DURATION", IntegerType, true) ::
      StructField("PRICE", DoubleType, true) ::
      StructField("INTERNATIONAL", BooleanType, true) :: Nil)
    val cdr = spark.read.option("header", "true").schema(schema).csv("../../data/call_cdr/year=1924/month=04/day=19")

    // First Spark job
    val long_cdr: DataFrame = cdr.filter(col("DURATION") > 60)  // Filter cdrs (narrow transformation: task)
    val callers: DataFrame = long_cdr.select("CALLER").distinct()  // Compute unique caller (wide transformation: stage)
    val num_distinct_long_callers: Long = callers.count()  // Count distinct long duration callers (action: job end)
    println(s"Long duration unique callers: $num_distinct_long_callers")

    // Second Spark job
    val prices: DataFrame = cdr.groupBy(col("INTERNATIONAL")).mean("PRICE")
    val national_price: Array[Row] = prices.filter(col("INTERNATIONAL") === false).collect()
    println(s"Precio medio de las llamadas nacionales: ${national_price(0)}")
  }
}