package org.ctl.parquet

import io.getquill.QuillSparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

object Main {

  val spark = SparkSession.builder()
    .config("spark.debug.maxToStringFields", "200")
    .appName("SparkQuillExample")
    .config("spark.sql.autoBroadcastJoinThreshold", 100)
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

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
      americans.map(a => HumanoidLivingSomewhere(a.firstName, a.lastName, a.address_id))
        .joinWith(addresses, $"whereHeLives_id" === $"id")
        .filter { tup => tup._2.current == true }
        .map { case (t, a) => s"Hello ${t.called} ${t.alsoCalled} of ${a.city}" }


//    val americanClients =
//      americans.map(a => HumanoidLivingSomewhere)
//        .joinWith(addresses, $"whereHeLives_id" === $"id")
//        .filter { tup => Boolean }
//        .map { => String }

    val combined =
      americans.map(a => HumanoidLivingSomewhere(a.firstName, a.lastName, a.address_id))

    americanClients.explain()
  }

  def usingQuill() = {
    import QuillSparkContext._

    val americans = liftQuery(spark.read.parquet("output/americans").as[American])
    val addresses = liftQuery(spark.read.parquet("output/addresses").as[Address])

//    val addressToSomeone = quote {
//      (t: Query[HumanoidLivingSomewhere]) =>
//        t.join(addresses).on((t, a) => a.id == t.whereHeLives_id).filter(_._2.current == true).map(_._1)
//    }

    val addressToSomeone = quote {
      (hum: Query[HumanoidLivingSomewhere]) =>
        for {
          t <- hum
          aa <- addresses.join(a => a.id == t.whereHeLives_id) //if (a.current == true)
        } yield t
    }

    val americanClients = quote {
      addressToSomeone(americans.map(a => HumanoidLivingSomewhere(a.firstName, a.lastName, a.address_id)))
    }

    run(americanClients).explain() //helloo
  }



  def main(args: Array[String]):Unit = {
    usingQuill()
  }
}
