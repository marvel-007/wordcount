package com.mocha.spark.kafkaTest

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * @author Yangxq
  * @version 2017/6/22 14:13
  */
object KafkaReadAndSave {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("KafkaReadAndSave").setMaster("local[5]")
    Logger.getRootLogger.setLevel(Level.WARN)
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("checkpoint")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.40.182:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group-logs-analysis-spark-access-1",
      "auto.offset.reset" -> "latest"
    )

    val topics = Array("topic-logs-analysis-weblogic-access-log")
    val kafkaConsumer = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
    val wordCountsDdd = kafkaConsumer.map(_.value()).flatMap(x => x.split("-")).flatMap(x => x.split(" ")).map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(5), Seconds(5))

    //调用DStream中的transform算子,可以进行数据转换
    val cleanedDStream = wordCountsDdd.transform(rdd => {
      rdd.map(pair => (pair._2, pair._1)).sortByKey(false).map(pair => (pair._2, pair._1))
    })
    //wordCounts.saveAsTextFiles("G:\\nginx-count.txt")
    cleanedDStream.foreachRDD(rdd => {
      println("===================================")
      rdd.take(5).foreach(println)
    })
    //kafkaConsumer.stop()
    ssc.start()
    ssc.awaitTermination()
  }
}
