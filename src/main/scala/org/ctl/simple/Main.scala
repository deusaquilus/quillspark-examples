package org.ctl.simple

import io.getquill.QuillSparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


case class American(firstName:String, lastName:String, addressId:Int)
case class Canadian(name:String, surname:String, residenceId:Int)
case class Yeti(gruntingSound:String, roaringSound:String, caveId:Int)
case class Address(id:Int, street:String, city:String, current: Boolean)

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
  val yetiList = List(
    Yeti("Aaargalah", "Gralala", 7),
    Yeti("Blargabar", "Grim-Grim-Grum", 8),
    Yeti("Cargabar", "Grayayaya", 9)
  )
  val addressesList = List(
    Address(1, "1st Ave", "New York", true),
    Address(2, "2st Ave", "New Jersey", true),
    Address(3, "Oregon Expressway", "Mountain View", true),
    Address(4, "1st Ave", "Toronto", true),
    Address(5, "2st Ave", "Montreal", true),
    Address(6, "3rd Ave", "Vancouver", true),
    Address(7, "1st Ave", "Kholat Syakhl", true),
    Address(8, "1st Ave", "Shaim", true),
    Address(9, "1st Ave", "Sverdlovskaya Oblast", true)
  )

  object QuillExamples {
    val americans = quote { liftQuery(americansList.toDS()) }
    val canadians = quote { liftQuery(canadiansList.toDS()) }
    val yeti = quote { liftQuery(yetiList.toDS()) }
    val addresses = quote { liftQuery(addressesList.toDS()) }



    val addressToSomeone = quote {
      (t: Query[HumanoidLivingSomewhere]) =>
        t.join(addresses).on((t, a) =>
          a.id == t.whereHeLives_id).map(_._1)
    }

    val americanClients = quote {
      addressToSomeone(americans.map(a => HumanoidLivingSomewhere(a.firstName, a.lastName, a.addressId)))
    }

    val canadianClients = quote {
      addressToSomeone(canadians.map(a => HumanoidLivingSomewhere(a.name, a.surname, a.residenceId)))
    }

    val yetiClients = quote {
      addressToSomeone(yeti.map(a => HumanoidLivingSomewhere(a.gruntingSound, a.roaringSound, a.caveId)))
    }

    def apply() = {
      //QuillSparkContext.translate(americanClients)
      run(americanClients).show()
    }
  }

  object DatasetExamples {
    val americans = americansList.toDS()
    val canadians = canadiansList.toDS()
    val yeti = yetiList.toDS()
    val addresses = addressesList.toDS()

    case class HumanoidLivingSomewhere(called:String, alsoCalled: String, whereHeLivesId:Int)



    def addressToSomeone(humanoidsLivingSomewhere: Dataset[HumanoidLivingSomewhere]) = {
      humanoidsLivingSomewhere
        .joinWith(addresses, $"id" === $"whereHeLivesId")
        .filter(ta => ta._2.current == true)
        .map { case (t, a) => s"Hello ${t.called} ${t.alsoCalled} of ${a.city}" }
    }

    val americanClients =
      addressToSomeone(
        americans.map(a => HumanoidLivingSomewhere(a.firstName, a.lastName, a.addressId))
      )
  }

  object FramelessExamples {
    implicit val session = spark
    import frameless.functions.lit
    import frameless.functions.nonAggregate.concat
    import frameless.TypedDataset

    val americans = TypedDataset.create(americansList)
    val canadians = TypedDataset.create(canadiansList)
    val yeti = TypedDataset.create(yetiList)
    val addresses = TypedDataset.create(addressesList)



    def addressToSomeone(humanoid: TypedDataset[HumanoidLivingSomewhere]) = {
      val joined = humanoid
          .joinInner(addresses) { humanoid('whereHeLives_id) === addresses('id) }

      joined.select(concat(
        lit("Hello "), joined.colMany('_1, 'called), lit(" "),
        joined.colMany('_1, 'alsoCalled), lit(" of "), joined.colMany('_2, 'city)))
    }

    def apply() = {
      val output =
        addressToSomeone(
          americans.select(americans('firstName), americans('lastName), americans('addressId))
            .deserialized.map{ case (name, age, whereHeLives_id ) =>
            HumanoidLivingSomewhere(
              name.asInstanceOf[String],
              age.asInstanceOf[String],
              whereHeLives_id.asInstanceOf[Int])
          }
        )

      output.dataset.show(false)
    }
  }

  object SqlExamples {
    val americans = americansList.toDF().createOrReplaceTempView("americans")
    val canadians = canadiansList.toDF().createOrReplaceTempView("canadians")
    val yeti = yetiList.toDF().createOrReplaceTempView("yetti")
    val addresses = addressesList.toDF().createOrReplaceTempView("addresses")

    def example() = {
      spark.sql(
        """
          |select concat('Hello ', t.called, ' ', t.alsoCalled, ' of ', a.city) as _1
          |from (
          |  select firstName as called, lastName as alsoCalled, addressId as whereHeLives_id
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

  object DataframeExamples {
    // Need to import them here because they conflict with the frameless functions
    import org.apache.spark.sql.functions._

    val americans = americansList.toDF()
    val canadians = canadiansList.toDF()
    val yeti = yetiList.toDF()
    val addresses = addressesList.toDF()

    americans.select($"firstName" as "called", $"lastName" as "alsoCalled", $"address_id" as "whereHeLives_id")
      .as("t")
      .join(addresses.as("a"), $"whereHeLivesId" === $"id")
      .select(
        concat(lit("Hello "), $"t.called", lit(" "), $"t.alsoCalled", lit(" of "), $"a.city"))
      .filter($"a.current" === lit(true))

    def addressToSomeone(humanoidLivingSomewhere: DataFrame) = {
      humanoidLivingSomewhere
        .as("t")
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
          yeti.select($"gruntSound" as "called", $"roarSound" as "alsoCalled", $"address_id" as "cave_id")
        )
    }

    def apply() = {
      addressToPeopleDF().show(false)

      //canadians.select(concat($"name", $"surname").as("blahblah")).show()
    }
  }




  def main(args: Array[String]): Unit = {
    //DataframeExamples()
    //QuillExamples()
    //FramelessExamples()
    SqlExamples()


  }
}
