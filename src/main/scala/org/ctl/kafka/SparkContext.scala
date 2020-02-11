package org.ctl.kafka

import org.apache.spark.sql.SparkSession

object SparkContext {

  val spark = SparkSession.builder()
    .config("spark.debug.maxToStringFields", "200")
    .appName("SparkQuillExample")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

}
