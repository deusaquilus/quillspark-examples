package org.ctl.kafka

import SparkContext._
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}
import spark.implicits._

import scala.util.Random
import org.apache.spark.sql.avro._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection

import scala.reflect.runtime.universe.TypeTag
import net.andreinc.mockneat.unit.user.Names.names
import org.apache.spark.sql.functions._

object KafkaConverter {
  def toKafkaDF[T: Encoder](ds: Dataset[T])(keyColumn: String, topic: String): DataFrame = {
    ds.toDF().select(ds(keyColumn), to_avro(struct($"*")) as "value", lit(topic) as "topic")
  }

  def fromKafkaDF[T: TypeTag: Encoder](df: DataFrame): Dataset[T] = { // TODO alternately provide Dataset[(K,T)] where kafka key is provided
    val schema = ScalaReflection.schemaFor[T].dataType
    val avroSchema = SchemaConverters.toAvroType(schema).toString(true)
    println(s"================ Avro Schema: ================\n${avroSchema}")

    val output =
      df.select($"key", from_avro($"value", avroSchema) as "value")
        .select($"value.*")
        .as[T]
    output
  }
}

case class Person(id: Int, firstName: String, lastName: String, age: Int)

object ReadFromStreamCam {
  def main(args: Array[String]):Unit = {
    val rawFrame =
      spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "blah")
        .option("startingOffsets", "earliest")
        //.option("endingOffsets", "latest")
        .load()

    val people = KafkaConverter.fromKafkaDF[Person](rawFrame)
    people.writeStream
      .format("console")
      .option("numRows", "1000")
      .outputMode("complete")
      .start().awaitTermination()
  }
}

object WriteToStreamCam {
  val rand = new Random()

  def main(args: Array[String]):Unit = {
    while(true) {
      //val camUnds = List(CamUnd.random()).toDS()
      //camUnds.
      //val personSchema = ScalaReflection.schemaFor[Person].dataType
      //SchemaConverters.toAvroType(personSchema).toString(true)

      val schema = ScalaReflection.schemaFor[Person].dataType
      val avroSchema = SchemaConverters.toAvroType(schema).toString(true)
      println("=============== Avro Schema ===============\n" + avroSchema)

      val person = Person(
        rand.nextInt(1000),
        names().first().get(), names().last().get(),
        rand.nextInt(120)
      )
      println(s"********* Record: ${person} *********")
      val peopleData = List(person).toDS()
      val peopleDataKafka = KafkaConverter.toKafkaDF(peopleData)("id", "blah")

      peopleDataKafka
        .write // writeStream is only when it was in a stream from the start
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        //.option("topic", "blah")
        .save()

      Thread.sleep(5000)
    }
  }
}

object WriteToStreamSM {
  def main(args: Array[String]):Unit = {

  }
}
