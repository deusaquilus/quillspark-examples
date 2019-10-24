package org.ctl.parquet

import org.apache.spark.sql.SparkSession

object JustSimplify {

  val spark = SparkSession.builder()
    .config("spark.debug.maxToStringFields", "200")
    .appName("SparkQuillExample")
    .config("spark.sql.autoBroadcastJoinThreshold", 100)
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  implicit val sqlContext = spark.sqlContext
  import sqlContext.implicits._

  def usingSuperOrder() = {
    import io.getquill.QuillSparkContext._

    val orders = spark.read.parquet("output/orders").as[SuperOrder]

    val skuPriceInfo =
      orders
        .map(o => (o.sku, BusinessMonoid(o.price)))
        .groupByKey(_._1)
        .reduceGroups(
          (a: (Int, BusinessMonoid), b: (Int, BusinessMonoid)) => (a._1, a._2 ++ a._2)
        )

    skuPriceInfo.write.parquet("price_sku_info")

    skuPriceInfo.explain()

  }

  def usingBoringOrder() = {
    import io.getquill.QuillSparkContext._

    val orders = spark.read.parquet("output/orders").as[SuperOrder]
    val boringOrders = run(liftQuery(orders).map(o => BoringOrder(o.sku, o.name, o.price)))

    val skuPriceInfo =
      boringOrders
        .map(o => (o.sku, BusinessMonoid(o.price)))
        .groupByKey(_._1)
        .reduceGroups(
          (a: (Int, BusinessMonoid), b: (Int, BusinessMonoid)) => (a._1, a._2 ++ a._2)
        )

    skuPriceInfo.explain()
  }

  def usingDataset() = {
    val orders = spark.read.parquet("output/orders").as[SuperOrder]
    val boringOrders = orders.map(o => BoringOrder(o.sku, o.name, o.price))
  }

  def usingDataFrame() = {
    val orders = spark.read.parquet("output/orders").as[SuperOrder]
    val boringOrders =
      orders.toDF().select($"sku", $"name", $"price").as[BoringOrder]
  }

  def main(args: Array[String]):Unit = {
    usingBoringOrder()
  }
}

case class BusinessMonoid(numSamples:Int, total:BigDecimal) {
  def ++(other: BusinessMonoid) =
    BusinessMonoid(
      this.numSamples + other.numSamples,
      this.total + other.total)
}
object BusinessMonoid {
  def apply(sample:BigDecimal): BusinessMonoid = BusinessMonoid(1, sample)
}
