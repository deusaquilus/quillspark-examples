package org.ctl.parquet

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataType, DecimalType}
import org.ctl.parquet.JustSimplify.spark

case class ChildNode(name:String)
case class Node(name:String, status:Int = 1, children: List[ChildNode] = List())

object OtherExamples {

  val spark = SparkSession.builder()
    .config("spark.debug.maxToStringFields", "200")
    .appName("SparkQuillExample")
    .config("spark.sql.autoBroadcastJoinThreshold", 100)
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  implicit val sqlContext = spark.sqlContext
  import sqlContext.implicits._
  import io.getquill.QuillSparkContext._

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.functions._


    // avg goes (38, 18) to (38,22) -> that's (20 left, 18 right) to (16 left, 22 right)
    // if we're taking 4 from the right to the left. We need to start with max of (38, 14)
    // so that the left side doesn't get truncated
    val ordersDF = spark.read.parquet("output/orders").withColumn("price", $"price".cast(DecimalType(33, 13)))
    val ordersDS = ordersDF.as[SuperOrder]
    println(ordersDS.schema)

    val orders = liftQuery(ordersDS)


    val americans = liftQuery(spark.read.parquet("output/americans").as[American])
    val canadians = liftQuery(spark.read.parquet("output/canadians").as[Canadian])
    val yeti = liftQuery(spark.read.parquet("output/yeti").as[Yeti])
    val addresses = liftQuery(spark.read.parquet("output/addresses").as[Address])
    val nodes = liftQuery(List[Node](Node("foo", 1, List(ChildNode("bar"), ChildNode("baz")))).toDS())


    def joins() = {
      // Applicative Joins
      val a1 = quote { yeti.join(addresses).on(_.caveId == _.id) }
      val a2 = quote { yeti.leftJoin(addresses).on(_.caveId == _.id) }
      run(a1)
      run(a2)

      // Implicit Joins
      val implicitJoins = quote {
        for {
          y <- yeti
          a <- addresses if (y.caveId == a.id)
        } yield (y, a)
      }
      run(implicitJoins)

      // Semi-Joins
      val cavelessYeti = quote {
        yeti.filter(y => !addresses.map(_.id).contains(y.caveId)) // need to fix up in file
      }
      run(cavelessYeti)
    }
    joins().show(false)

    def groupByExample() = {

      val groupedOrders = quote {
        // Group-By
        orders.groupBy(_.sku).map {
          case (sku, orders) => (sku, orders.map(_.price).avg)
        }
      }
      run(groupedOrders)
    }
    groupByExample().show(false)

    def concatMapExample() = {
      // Concat-Map
      val nodesChildren = quote {
        (ns: Query[Node]) => ns.concatMap(n => n.children)
      }
      run(nodesChildren(nodes))
    }
    concatMapExample().show(false)

    def unionExample() = {
      // Union/UnionAll
      val americansAndCanadians = quote {
        americans.map(a => (a.firstName, a.lastName)) unionAll canadians.map(a => (a.name, a.surname))
      }
      run(americansAndCanadians)
    }
    unionExample().show(false)

    def dropTake() = {
      val thirdAndFourthYeti = quote {
        yeti.take(2)
        // yeti.distinct.take(2) // causes a stack overflow. Check if happens with other DBs. Need to file a bug.
      }
      run(thirdAndFourthYeti)
    }
    dropTake().show(false)


    def udfExample() = {
      // Using Spark UDFs
      spark.udf.register("businessLogicUdf", (str:String) => str + "-suffix")
      val businessLogicUdf = quote {
        (str: String) => infix"businessLogicUdf(${str})".as[String]
      }
      val usingUdf = quote {
        yeti.map(y => businessLogicUdf(y.gruntingSound))
      }
      run(usingUdf)
    }
    udfExample().explain()
    udfExample().show(false)

    def geometricMeanExample() = {
      // https://docs.databricks.com/spark/latest/spark-sql/udaf-scala.html

      // User Defined Aggregation Functions (UDAFs)
      spark.udf.register("geomMean", new GeometricMean)
      val geomMean = quote {
        (q: Query[BigDecimal]) => infix"geomMean(${q})".as[BigDecimal]
      }

      val groupedOrders = quote {
        // Group-By
        orders.groupBy(_.sku).map {
          case (sku, orders) => (sku, geomMean(orders.map(_.price)))
        }
      }
      run(groupedOrders)
    }
    geometricMeanExample().explain()
    geometricMeanExample().show(false)




  }
}
