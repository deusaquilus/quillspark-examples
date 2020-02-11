package org.ctl.parquet

import org.apache.spark.api.java.function.{FilterFunction, MapFunction}
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object Main2 {

  val spark = SparkSession.builder()
    .config("spark.debug.maxToStringFields", "200")
    .appName("SparkQuillExample")
    .config("spark.sql.autoBroadcastJoinThreshold", 100)
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  import spark.sqlContext.implicits._
  implicit val sc = spark.sqlContext
  import io.getquill.QuillSparkContext._

  def main(args: Array[String]):Unit = {
    val americansDF: DataFrame = spark.read.parquet("output/americans")
    val addressesDF: DataFrame = spark.read.parquet("output/addresses")

    val firstNamesDR: DataFrame = americansDF.select($"firstName")



    val resultDR: DataFrame =
      americansDF
        .join(addressesDF, $"id" === $"address_id")
        .filter($"lastName" === "Kellogg")
        .select($"firstName")

    //resultDR.explain()

    val americans: Dataset[American] = americansDF.as[American]
    val addresses: Dataset[Address] = addressesDF.as[Address]



    val americanNamesDS1: Dataset[String] =
      americans
        .map(new MapFunction[American, String] {
        override def call(value: American): String = value.firstName
      }, Encoders.STRING)
      .filter(new FilterFunction[String] {
        override def call(value: String): Boolean = value == "Joe"
      })

    // americans.joinWith(addresses, (am, addr) => a.id == address.address_id)
    /*
    americans.joinWith(addresses,
      JoinFilter[Americans, Addresss] {
        def call(am: Americans, addr: Addresses): Boolean = am.id == addr.address_id
      }
     */



    //joesNamesDS.explain()

    val americansAndAddress:Dataset[(American, Address)] =
      americans.joinWith(addresses, $"id" === $"address_id")
      //(aa: American, a: Address) => a.id == aa.ownerFk

    val blah = "foo"

    val joesNamesDS: Dataset[String] =
      americans
        .map(american => american.firstName)
        .filter(str => str == "Joes")

    val joesNamesDSQuill: Dataset[String] =
      run {
        liftQuery(americans)
          .map(american => american.firstName)
          .filter(str => str == "Joes")
      }
    joesNamesDSQuill.explain()

    val addressesQR = quote { liftQuery(addresses) }
    val americansQR = quote { liftQuery(americans) }

    val americansAndAddressesQR =
      run {
        for {
          a <- americansQR
          addr <- addressesQR.join(addr => a.address_id == addr.id)
        } yield (a, addr)
      }

    //americans.show()
  }
}
