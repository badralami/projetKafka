package com.bnpparibas.processing

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.{Encoder, SparkSession}

case class Nested(name: String, tags: List[Tag])
case class Tag(a: String, b: String, c: Int)
case class Flattened(name: String, a: String, b: String, c: Int)

object Applicationreadjson {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .appName("Creating spark session")
      .master("local[*]")
      .getOrCreate()

/*
    val path = "/home/fouad/BigApps/kafkaBigapps/projetKafka/processing/src/main/resources/JsonTopic1.json"
    //val path = "/home/fouad/BigApps/kafkaBigapps/projetKafka/processing/src/main/resources/interactions.csv"

    val df = spark.read.option("multiline", "true").json(path)
    df.show()
    println(df)
    println("333333333333")
*/

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val nestedEncoder: Encoder[Nested] = Encoders.bean(classOf[Nested])
    val nestedSchema = nestedEncoder.schema
    val tpath = "/home/fouad/BigApps/kafkaBigapps/projetKafka/processing/src/main/resources/Client.json"

    val nestedDS =
      spark
        .read
        .schema(nestedSchema)
        .json(tpath)
        .as[Nested](nestedEncoder)

    println("1111111111111")
    nestedDS.show()

    val flattenedDS = for {
      nested <- nestedDS
      tag <- nested.tags
    } yield Flattened(nested.name, tag.a, tag.b, tag.c)
    // flattenedDS: Dataset[Flattened] = [name: string, a: string, b: string, c: integer]

    println("2222222222222")
    flattenedDS.printSchema()
    println("333333333333")
    // root
    //  |-- name: string (nullable = true)
    //  |-- a: string (nullable = true)
    //  |-- b: string (nullable = true)
    //  |-- c: integer (nullable = false)

    println("222222222222")
    flattenedDS.show()
    println("444444444444")

  }

}
