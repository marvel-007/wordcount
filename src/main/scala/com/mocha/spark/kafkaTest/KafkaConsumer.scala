package com.mocha.spark.kafkaTest

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * @author Yangxq
  * @version 2017/6/22 14:13
  */
object KafkaConsumer {

    /**
      * kafka配置信息
      */
    val kafkaParams1 = Map[String, Object](
        "bootstrap.servers" -> "192.168.1.250:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "group-logs-analysis-spark-access-08-01-1",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val kafkaParams2 = Map[String, Object](
        "bootstrap.servers" -> "192.168.40.182:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "group-logs-analysis-spark-access-08-01-2",
        "auto.offset.reset" -> "latest"
    )

    val kafkaParams3 = Map[String, Object](
        "bootstrap.servers" -> "192.168.40.182:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "group-logs-analysis-spark-access-08-01-3",
        "auto.offset.reset" -> "latest"
    )


    /**
      * 创建spark上下文
      *
      * @param appname
      * @return
      */
    def initStreamingContext(appname: String, master: String, batchDuration: Int) = {
        val sparkConf = new SparkConf().setAppName(appname).setMaster(master)
        Logger.getRootLogger.setLevel(Level.WARN)
        val ssc = new StreamingContext(sparkConf, Seconds(batchDuration))
        ssc.checkpoint("checkpoint")
        ssc
    }


    /**
      * 初始化kafka消费者
      *
      * @param ssc
      * @param kafkaParams
      * @param topics
      * @return
      */
    def initKafkaConsumer(ssc: StreamingContext, kafkaParams: Map[String, Object], topics: Array[String]) = {
        val kafkaConsumer = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
        kafkaConsumer
    }


    /**
      * 消费者统计输出
      *
      * @param kafkaConsumer
      * @param timelong
      * @param period
      * @param top
      */
    def consumerCount(kafkaConsumer: InputDStream[ConsumerRecord[String, String]], timelong: Int, period: Int, top: Int, name: String) = {
        val wordCountsDdd = kafkaConsumer.map(_.value()).flatMap(x => x.split(" ")).map(x => (x, 1L)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(timelong), Seconds(period))
        //调用DStream中的transform算子,可以进行数据转换
        wordCountsDdd.transform(rdd => {
            rdd.map(pair => (pair._2, pair._1)).sortByKey(false).map(pair => (pair._2, pair._1))
        }).foreachRDD(rdd => {
            println("============" + name + "============")
            rdd.take(top).foreach(println)
        })
    }


    def main(args: Array[String]) {
        val ssc = initStreamingContext("KafkaCount", "local[10]", 5)

        val topics1 = Array("topic-kafka-test-2017")
        val topics2 = Array("topic-logs-analysis-weblogic-access-log")
        val topics3 = Array("topic-logs-analysis-weblogic-access-log")

        val kafkaConsumer1 = initKafkaConsumer(ssc, kafkaParams1, topics1)
        consumerCount(kafkaConsumer1, 10, 5, 5, "c1")

        /*    val kafkaConsumer2 = initKafkaConsumer(ssc, kafkaParams2, topics2)
            consumerCount(kafkaConsumer2, 10, 5, 5, "c2")

            val kafkaConsumer3 = initKafkaConsumer(ssc, kafkaParams3, topics3)
            consumerCount(kafkaConsumer3, 10, 5, 5, "c3")*/
        ssc.start()
        ssc.awaitTermination()
    }
}
