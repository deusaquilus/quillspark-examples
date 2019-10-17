package org.ctl.simple

import io.getquill.QuillSparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

case class American(firstName:String, lastName:String, addressId:Int)
case class Canadian(name:String, surname:String, residenceId:Int)
case class Yetti(gruntingSound:String, roaringSound:String, caveId:Int)
case class Address(id:Int, street:String, city:String)

case class HumanoidLivingSomewhere(called:String, alsoCalled: String, whereHeLives_id:Int)


object Main {

  val spark = SparkSession.builder()
    .config("spark.debug.maxToStringFields", "200")
    .appName("SparkQuillExample")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  implicit val sqlContext = spark.sqlContext
  import sqlContext.implicits._
  import QuillSparkContext._

  val americansList = List(
    American("John", "James", 1),
    American("Joe", "Bloggs", 2),
    American("Roe", "Roggs", 3)
  )
  val canadiansList = List(
    Canadian("Jim", "Jones", 4),
    Canadian("Tim", "Bones", 5),
    Canadian("Pim", "Cones", 6)
  )
  val yettiList = List(
    Yetti("Aaargalah", "Gralala", 7),
    Yetti("Blargabar", "Grim-Grim-Grum", 8),
    Yetti("Cargabar", "Grayayaya", 9)
  )
  val addressesList = List(
    Address(1, "1st Ave", "New York"),
    Address(2, "2st Ave", "New Jersey"),
    Address(3, "Oregon Expressway", "Mountain View"),
    Address(4, "1st Ave", "Toronto"),
    Address(5, "2st Ave", "Montreal"),
    Address(6, "3rd Ave", "Vancouver"),
    Address(7, "1st Ave", "Kholat Syakhl"),
    Address(8, "1st Ave", "Shaim"),
    Address(9, "1st Ave", "Sverdlovskaya Oblast")
  )

  object QuillExamples {
    val americans = quote { liftQuery(americansList.toDS()) }
    val canadians = quote { liftQuery(canadiansList.toDS()) }
    val yetti = quote { liftQuery(yettiList.toDS()) }
    val addresses = quote { liftQuery(addressesList.toDS()) }



    val addressToSomeone = quote {
      (t: Query[HumanoidLivingSomewhere]) =>
        t.join(addresses).on((t, a) => a.id == t.whereHeLives_id).map(_._1)
    }

    val americanClients = quote {
      addressToSomeone(americans.map(a => HumanoidLivingSomewhere(a.firstName, a.lastName, a.addressId)))
    }

    val canadianClients = quote {
      addressToSomeone(canadians.map(a => HumanoidLivingSomewhere(a.name, a.surname, a.residenceId)))
    }

    val yettiClients = quote {
      addressToSomeone(yetti.map(a => HumanoidLivingSomewhere(a.gruntingSound, a.roaringSound, a.caveId)))
    }

    def apply() = {
      //QuillSparkContext.translate(americanClients)
      run(americanClients).show()
    }
  }

  object DatasetExamples {
    val americans = americansList.toDS()
    val canadians = canadiansList.toDS()
    val yetti = yettiList.toDS()
    val addresses = addressesList.toDS()

    case class HumanoidLivingSomewhere(called:String, alsoCalled: String, whereHeLivesId:Int)

    def addressToSomeone(humanoidsLivingSomewhere: Dataset[HumanoidLivingSomewhere]) = {
      humanoidsLivingSomewhere
        .joinWith(addresses, $"id" === $"whereHeLivesId")
        .map { case (t, a) => s"Hello ${t.called} ${t.alsoCalled} of ${a.city}" }
    }

    val americanClients =
      addressToSomeone(
        americans.map(a => HumanoidLivingSomewhere(a.firstName, a.lastName, a.addressId))
      )
  }

  object DataframeExamples {
    val americans = americansList.toDF()
    val canadians = canadiansList.toDF()
    val yetti = yettiList.toDF()
    val addresses = addressesList.toDF()

    def addressToSomeone(humanoidLivingSomewhere: DataFrame) = {
      humanoidLivingSomewhere.as("t")
        .join(addresses.as("a"), $"whereHeLivesId" === $"id")
        .select(
          concat(lit("Hello "), $"t.called", lit(" "), $"t.alsoCalled", lit(" of "), $"a.city"))
        .filter($"a.current" === lit(true))
    }

    def addressToPeopleDF() = {
    addressToSomeone(
      americans.select($"firstName" as "called", $"lastName" as "alsoCalled", $"address_id" as "whereHeLives_id")
    )
    addressToSomeone(
      canadians.select($"name" as "called", $"surname" as "alsoCalled", $"address_id" as "residence_id")
    )
    addressToSomeone(
      yetti.select($"gruntSound" as "called", $"roarSound" as "alsoCalled", $"address_id" as "cave_id")
    )
    }

    def apply() = {
      addressToPeopleDF().show(false)

      //canadians.select(concat($"name", $"surname").as("blahblah")).show()
    }
  }




  def main(args: Array[String]): Unit = {
    //DataframeExamples()
    QuillExamples()


  }
}
