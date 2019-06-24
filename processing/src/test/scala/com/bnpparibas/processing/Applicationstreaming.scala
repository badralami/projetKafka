package com.bnpparibas.processing

import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Applicationstreaming {

  def main(args: Array[String]) {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> Class.forName("org.apache.kafka.common.serialization.StringDeserializer"),
      "value.deserializer" -> Class.forName("org.apache.kafka.common.serialization.StringDeserializer"),
      "group.id" -> "group-topic",
      "client.id" -> "balami",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val sparkConf = new SparkConf().setAppName("Kafka Test").setMaster("local[*]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(20))

    val topics = Array("topic1")
    // val offsets = Map( new TopicPartition("topicinteraction1", 0) -> 2L, new TopicPartition("topicinteraction2", 0) -> 2L)

    val preferredHosts = LocationStrategies.PreferConsistent
    val dstream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      preferredHosts,
      ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    // ConsumerStrategies.Subscribe[String, String](topics, kafkaParams,offsets)

    val rddstream = dstream.map(record => ( record.value)).map( line => ( line.split(";")) )

    rddstream.foreachRDD((rdd) => {

      val sqlContext = new SQLContext(rdd.sparkContext)
      import sqlContext.implicits._

      val requestsDataFrame = rdd.map(w => ( w(0)+"rrrrrrrrrrrrrrrrrr", w(1), w(2), w(3), w(4))).toDF()
      requestsDataFrame.show()
    })

    streamingContext.start()
    streamingContext.awaitTermination()

  }

}
