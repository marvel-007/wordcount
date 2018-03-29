package com.mocha.spark.kafka

import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.util.Random

/**
  * Kafka数据源生产者
  *
  * @author Yangxq
  * @version 2017/5/15 11:46
  */
object KafkaWordProducer {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> <messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }
    //第1个参数h1:19092是Kafka的broker的地址
    // 第2个参数wordsender是topic的名称，我们在KafkaWordCount.scala代码中已经把topic名称写死了
    // 所以，KafkaWordCount程序只能接收名称为"topic-test-170515-word-count"的topic
    // 第3个参数“3”表示每秒发送3条消息
    // 第4个参数“5”表示，每条消息包含5个单词（实际上就是5个整数）
    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args
    // Zookeeper connection properties
    val props = new util.HashMap[String, Object]()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    // Send some messages
    while (true) {
      (1 to messagesPerSec.toInt).foreach { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => Random.nextInt(10).toString).mkString(" ")
        print(str)
        println()
        val message = new ProducerRecord[String, String](topic, null, str)
        producer.send(message)
      }
      Thread.sleep(1000)
    }
  }
}
