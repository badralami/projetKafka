package com.bnpparibas.processing

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object ApplicationSpark {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("KafkaTest").setMaster("local[*]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(2))

    val kafkastream = KafkaUtils.createStream( streamingContext, "localhost:2181", "spark-streaming-consumer-group" , Map("topic2" -> 5), StorageLevel.MEMORY_ONLY_SER)

    kafkastream.print()

    streamingContext.start
    streamingContext.awaitTermination()
  }

}
