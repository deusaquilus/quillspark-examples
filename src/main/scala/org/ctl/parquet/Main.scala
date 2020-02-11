package org.ctl.parquet

import io.getquill.QuillSparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import org.ctl.SetRootLogger




object Main {

  val spark = SparkSession.builder()
    .config("spark.debug.maxToStringFields", "200")
    .appName("SparkQuillExample")
    .config("spark.sql.autoBroadcastJoinThreshold", 100)
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  SetRootLogger.setLevel()

  implicit val sqlContext = spark.sqlContext
  import sqlContext.implicits._

  def usingDataFrame() = {

    val americans = spark.read.parquet("output/americans")
    val addresses = spark.read.parquet("output/addresses")

    def addressToSomeone(humanoidLivingSomewhere: DataFrame) = {
      humanoidLivingSomewhere.as("t")
        .join(addresses.as("a"), $"whereHeLives_id" === $"id")
        .select(
          concat(lit("Hello "), $"t.called", lit(" "), $"t.alsoCalled", lit(" of "), $"a.city"))
        .filter($"a.current" === lit(true))
    }

    val output =
    addressToSomeone(
      americans.select($"firstName" as "called", $"lastName" as "alsoCalled", $"address_id" as "whereHeLives_id")
    )

    output.explain()
  }

  def usingDataset() = {
    val americans = spark.read.parquet("output/americans").as[American]
    val addresses = spark.read.parquet("output/addresses").as[Address]


    def addressToSomeone(humanoidsLivingSomewhere: Dataset[HumanoidLivingSomewhere]) = {
      humanoidsLivingSomewhere
        .joinWith(addresses, $"whereHeLives_id" === $"id")
        .filter { tup => tup._2.current == true }
        .map { case (t, a) => s"Hello ${t.called} ${t.alsoCalled} of ${a.city}" }
    }

    val americanClients =
      addressToSomeone(americans.map(a => HumanoidLivingSomewhere(a.firstName, a.lastName, a.address_id)))

    americanClients.explain()
  }

  def usingQuill() = {



    import QuillSparkContext._

    val americansDS = spark.read.parquet("output/americans").as[American]
    val addressesDS = spark.read.parquet("output/addresses").as[Address]
    val americans = liftQuery(americansDS)
    val addresses = liftQuery(addressesDS)

    val addressToSomeone = quote {
      (humanoids: Query[HumanoidLivingSomewhere]) =>
        for {
          h <- humanoids
          a <- addresses.join(a => a.id == h.whereHeLives_id) if (a.current)
        } yield "Hello " + h.called + " " + h.alsoCalled + " of " + a.city
    }

    val output = quote {
      addressToSomeone(
        americans.map(am =>
          HumanoidLivingSomewhere(am.firstName, am.lastName, am.address_id)
        )
      )
    }

    val yetiOfSomeplace: Dataset[String] =
      run(output)


    val superItem = spark.read.parquet("output/americans").as[American]
    //superItem.agg
  }


  object SqlExamples {
    spark.read.parquet("output/americans").as[American].createOrReplaceTempView("americans")
    spark.read.parquet("output/addresses").as[Address].createOrReplaceTempView("addresses")

    def example() = {
      spark.sql(
        """
          |select concat('Hello ', t.called, ' ', t.alsoCalled, ' of ', a.city) as _1
          |from (
          |  select firstName as called, lastName as alsoCalled, address_id as whereHeLives_id
          |  from americans
          |) as t
          |join addresses a on (t.whereHeLives_id = a.id)
          |where a.current = true
          |""".stripMargin
      )
    }

    def apply() = {
      example().show(false)
      example().explain()
    }
  }

  def main(args: Array[String]):Unit = {
    //usingQuill()
    //usingDataFrame()
    SqlExamples()
  }
}
