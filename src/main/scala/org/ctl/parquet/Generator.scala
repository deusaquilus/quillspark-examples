package org.ctl.parquet

import org.apache.spark.sql.SparkSession


object Generator {

  val spark = SparkSession.builder()
    .config("spark.debug.maxToStringFields", "200")
    .appName("SparkQuillExample")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  implicit val sqlContext = spark.sqlContext
  import sqlContext.implicits._

  def write() = {
    (1 to 100000).map(_ => American.random()).toDS().write.parquet("output/americans")
    (1 to 100000).map(_ => Canadian.random()).toDS().write.parquet("output/canadians")
    (1 to 100000).map(_ => Yeti.random()).toDS().write.parquet("output/yeti")
    (1 to 100000).map(_ => Address.random()).toDS().write.parquet("output/addresses")
  }

  def writeOrder() = {
    (1 to 100000).map(_ => SuperOrder.random()).toDS().write.parquet("output/orders")
  }

  def read() = {
    spark.read.parquet("output/americans").show(false)
    spark.read.parquet("output/addresses").show(false)
  }

  def main(args: Array[String]): Unit = {
    write()
    //Thread.sleep(5000)
    writeOrder()
  }
}
