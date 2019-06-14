package com.bnpparibas.processing

object ApplicationSpark {

  def main(args: Array[String]) {

/*
    val sparkConf = new SparkConf().setAppName("KafkaTest").setMaster("local[2]")
    val streamingContext = new StreamingContext(sparkConf, Seconds(2))

    val kafkastream = KafkaUtils.createStream( streamingContext, "localhost:2181", "spark-streaming-consumer-group" , Map("topictest" -> 5), StorageLevel.MEMORY_ONLY_SER)

    kafkastream.print()

    streamingContext.start
    streamingContext.awaitTermination()
    */

  }

}