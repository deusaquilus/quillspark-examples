package org.ctl.complex

import io.getquill.QuillSparkContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class American(firstName:String, lastName:String, address_id:Int)
case class Address(id:Int, street:String, city:String, state:String, zip:Int, rid:Int, current:Boolean = true)
case class ResidenceUnit(id:Int, class_id:Int, zone_id:Int, kdd:String)
case class ResidenceClass(kdd:Int, foobar:String, class_id:Int, barbaz:String)
case class ZoningDesignation(rzid:Int, cid:Int, zone_type:String, kdd:String)


case class Canadian(name:String, surname:String, residenceId:Int)
case class Yeti(gruntingSound:String, roaringSound:String, caveId:Int)

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

  val residenceUnitList = List[ResidenceUnit]()
  val residenceClassList = List[ResidenceClass]()
  val zoningDesignationList = List[ZoningDesignation]()

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
    Address(1, "1st Ave", "New York", "NY", 123, 1),
    Address(2, "2st Ave", "New Jersey", "NY", 123, 1)
  )


  """
    |
    |""".stripMargin


  // language=SQL
  """
    |SELECT
    |  t.called || ' ' || t.alsoCalled || 'of' || a.city,
    |  CASE
    |    WHEN zd.zone_type = 'K' THEN 'StandardCategory'
    |    WHEN zd.zone_type = 'N' AND rc.barbaz = 'GT' THEN 'NonStandardCategory'
    |    ELSE 'UnknownCategory'
    |  END as zoning_category,
    |  CASE
    |    WHEN ru.kdd = 'IK' THEN 'Insanity'
    |    WHEN zd.kdd = 'N' AND rc.barbaz = 'GTT' THEN 'MoreInsanity'
    |    ELSE 'I_Dont_Even_Know_What_Goes_Here'
    |  END as zoning_category
    |FROM @humanoidLivingSomewhere t
    |JOIN Addresses a on t.address_id = a.id
    |JOIN ResidenceUnit ru on a.rid = ru.id
    |JOIN ResidenceClass rc on ru.class_id = rc.class_id
    |JOIN ZoningDesignation zd on ru.zone_id = zd.rzid and zd.cid = rc.class_id
    |
    |""".stripMargin



  object QuillExamples {

    import QuillSparkContext._

    val americans = liftQuery(americansList.toDS())
    val canadians = liftQuery(canadiansList.toDS())
    val yeti = liftQuery(yetiList.toDS())
    val addresses = liftQuery(addressesList.toDS())
    val residenceUnit = liftQuery(residenceUnitList.toDS())
    val residenceClass = liftQuery(residenceClassList.toDS())
    val zoningDesignation = liftQuery(zoningDesignationList.toDS())


    val insaneJoin = quote { (humanoid: Query[HumanoidLivingSomewhere]) =>
      for {
        h <- humanoid
        a <- addresses.join(a => a.id == h.whereHeLives_id)
        ru <-residenceUnit.join(ru => a.rid == ru.id)
        rc <- residenceClass.join(rc => ru.class_id == rc.class_id)
        zd <- zoningDesignation.join(zd =>
                ru.zone_id == zd.rzid && zd.cid == rc.class_id
        )
        if (a.current == true)
      } yield (
        "Hello " +
          h.called + " " +
          h.alsoCalled + " of " +
          a.city,
          if (zd.zone_type == "K") "StandardCategory"
          else if (zd.zone_type == "N" && rc.barbaz == "GT") "NonStandardCategory"
          else "UnknownCategory",
          if (ru.kdd == "IK") "Insanity"
          else if (zd.kdd == "N" && rc.barbaz == "GTT") "MoreInsanity"
          else "I_Dont_Even_Know_What_Goes_Here",
        )
    }

    /*
      CASE
    WHEN zd.zone_type = 'K' THEN 'StandardCategory'
    WHEN zd.zone_type = 'N' AND rc.barbaz = 'GT' THEN 'NonStandardCategory'
    ELSE 'UnknownCategory'
  END as zoning_category,
  CASE
    WHEN ru.kdd = 'IK' THEN 'Insanity'
    WHEN zd.kdd = 'N' AND rc.barbaz = 'GTT' THEN 'MoreInsanity'
    ELSE 'I_Dont_Even_Know_What_Goes_Here'
  END as zoning_category
     */

  }

  object FramelessExamples {
    implicit val session = spark
    import spark.implicits._
    import frameless.functions.lit
    import frameless.functions.nonAggregate.{concat, when}
    import frameless.TypedDataset
    import frameless.syntax._

    val americans = TypedDataset.create(americansList)
    val canadians = TypedDataset.create(canadiansList)
    val yeti = TypedDataset.create(yetiList)
    val addresses = TypedDataset.create(addressesList)
    val residenceUnit = TypedDataset.create(residenceUnitList)
    val residenceClass = TypedDataset.create(residenceClassList)
    val zoningDesignation = TypedDataset.create(zoningDesignationList)



    def addressToSomeone(humanoid: TypedDataset[HumanoidLivingSomewhere]) = {
      val j1 = humanoid.joinInner(addresses) { humanoid('whereHeLives_id) === addresses('id) }
      val j2 = j1.joinInner(residenceUnit) { j1.colMany('_2, 'rid) === residenceUnit('id) }
      val j3 = j2.joinInner(residenceClass) { j2.colMany('_2, 'class_id) === residenceClass('class_id) }
      val j4 = j3.joinInner(zoningDesignation) {
        (j3.colMany('_1, '_2, 'zone_id) === zoningDesignation('rzid)) &&
          (zoningDesignation('rzid) === j3.colMany('_1, '_2, 'class_id))
      }

      type OUT = ((((HumanoidLivingSomewhere, Address), ResidenceUnit), ResidenceClass), ZoningDesignation)
      j4.select(
        concat(
          lit("Hello "), j4.colMany('_1, '_1, '_1, '_1, 'called), lit(" "),
          j4.colMany('_1, '_1, '_1, '_1, 'alsoCalled), lit(" of "), j4.colMany('_1, '_1, '_1, '_2, 'city)
        ),
        when(j4.colMany('_2, 'zone_type) === "K", lit[String, OUT]("StandardCategory"))
          .when((j4.colMany('_2, 'zone_type) === "N") && (j4.colMany('_1, '_2, 'barbaz) === "GT"),
            lit("NonStandardCategory"))
          .otherwise( lit("UnknownCategory")),
        when(j4.colMany('_1, '_1, '_2, 'kdd) === "IK",  lit[String, OUT]("Insanity"))
          .when((j4.colMany('_2, 'kdd) === "N") && (j4.colMany('_1, '_2, 'barbaz) === "GTT"),
            lit("MoreInsanity"))
          .otherwise(lit("I_Dont_Even_Know_What_Goes_Here"))
      )
    }

    def apply() = {
      val output =
        addressToSomeone(
          americans.select(americans('firstName), americans('lastName), americans('address_id))
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

  object DatasetExamples {
    import org.apache.spark.sql.functions._

    val americans = americansList.toDS()
    val canadians = canadiansList.toDS()
    val yeti = yetiList.toDS()
    val addresses = addressesList.toDS()
    val residenceUnit = residenceUnitList.toDS()
    val residenceClass = residenceClassList.toDS()
    val zoningDesignation = zoningDesignationList.toDS()

    def insaneJoin(humanoidsLivingSomewhere: Dataset[HumanoidLivingSomewhere]) =
      humanoidsLivingSomewhere.as("t")
        .joinWith(addresses.as("a"), $"whereHeLives_id" === $"id")
        .joinWith(residenceUnit.as("ru"), $"_2.rid" === $"ru.id")
        .joinWith(residenceClass.as("rc"), $"_2.class_id" === $"rc.class_id")
        .joinWith(zoningDesignation.as("zd"),
          ($"_1._2.zone_id" === "zd.rzid") &&
          ($"zd.cid" === $"_1._2.class_id")
        )
        .map { case ((((t, a), ru), rc), zd) => (
            s"Hello ${t.called} ${t.alsoCalled} of ${a.city}",
            if (zd.zone_type == "K") "StandardCategory"
            else if (zd.zone_type == "N" && rc.barbaz == "GT")
              "NonStandardCategory"
            else
              "UnknownCategory",
            if (ru.kdd == "IK") "Insanity"
            else if (zd.kdd == "N" && rc.barbaz == "GT")
              "MoreInsanity"
            else
              "I_Dont_Even_Know_What_Goes_Here"
          )
        }

    val americanClients =
      insaneJoin(
        americans.map(a => HumanoidLivingSomewhere(a.firstName, a.lastName, a.address_id))
      )

      """
        |  CASE
        |    WHEN zd.zone_type = 'K' THEN 'StandardCategory'
        |    WHEN zd.zone_type = 'N' AND rc.barbaz = 'GT' THEN 'NonStandardCategory'
        |    ELSE 'UnknownCategory'
        |  END as zoning_category1,
        |  CASE
        |    WHEN ru.kdd = 'IK' THEN 'Insanity'
        |    WHEN zd.kdd = 'N' AND rc.barbaz = 'GTT' THEN 'MoreInsanity'
        |    ELSE 'I_Dont_Even_Know_What_Goes_Here'
        |  END as zoning_category2
        |""".stripMargin
  }

  object DataFrameExamples {
    import org.apache.spark.sql.functions._

    val americans = americansList.toDF()
    val canadians = canadiansList.toDF()
    val yeti = yetiList.toDF()
    val addresses = addressesList.toDS()
    val residenceUnit = residenceUnitList.toDS()
    val residenceClass = residenceClassList.toDS()
    val zoningDesignation = zoningDesignationList.toDS()

    def insaneJoin(humanoidLivingSomewhere: DataFrame) =
      humanoidLivingSomewhere.as("t")
      .join(addresses.as("a"), $"t.whereHeLives_id" === $"a.id")
      .join(residenceUnit.as("ru"), $"a.rid" === $"ru.id")
      .join(residenceClass.as("rc"), $"ru.class_id" === $"rc.class_id")
      .join(zoningDesignation.as("zd"),
        ($"ru.zone_id" === "zd.rzid") &&
        ($"zd.cid" === $"rc.class_id")
      )
      .select(
        concat(
          lit("Hello "), $"t.called", lit(" "), $"t.alsoCalled",
          lit(" of "), $"a.city"),
        when($"zd.zone_type" === lit("K"), "StandardCategory")
          .when(($"zd.zone_type" === lit("N")) && ($"rc.barbaz" === lit("GT")),
            "NonStandardCategory")
          .otherwise("UnknownCategory")
          .as("zoning_category1"),
        when($"ru.kdd" === lit("IK"), "Insanity")
          .when(($"zd.kdd" === lit("N")) && ($"rc.barbaz" === lit("GTT")),
            "MoreInsanity")
          .otherwise("I_Dont_Even_Know_What_Goes_Here")
          .as("zoning_category2")
      )

    val americanClients =
      insaneJoin(
        americans.select($"firstName" as "called", $"lastName" as "alsoCalled", $"address_id" as "whereHeLives_id")
      )

    val canadianClients =
      insaneJoin(
        canadians.select($"name" as "called", $"surname" as "alsoCalled", $"address_id" as "residence_id")
      )

    val yetiClients =
      insaneJoin(
        yeti.select($"gruntSound" as "called", $"roarSound" as "alsoCalled", $"address_id" as "cave_id")
      )
  }



  def main(args: Array[String]):Unit = {
    DatasetExamples.americanClients.show()

  }
}
