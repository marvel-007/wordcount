package com.mocha.spark.kafkaTest

import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.{Assign, Subscribe}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * @author Yangxq
  * @version 2017/6/22 14:13
  */
object KafkaConsumer {

    var offsetRanges = Array[OffsetRange]()
    val topics1 = Array("test-five-partition-test-180504")
    val group = "group-logs-analysis-spark-access-08-01-1"
    val topicDirs = new ZKGroupTopicDirs(group, topics1(0)) //创建一个 ZKGroupTopicDirs 对象，对保存
    var fromOffsets: Map[TopicPartition, scala.Long] = Map() //如果 zookeeper 中有保存 offset，我们会利用这个 offset 作为 kafkaStream 的起始位置

    val zkHost = "192.168.40.23:2181"
    val kafkaHost = "192.168.40.23:9092"
    val zkClient = new ZkClient(zkHost)
    val children = zkClient.countChildren(s"${topicDirs.consumerOffsetDir}") //查询该路径下是否字节点（默认有字节点为我们自己保存不同 partition 时生成的）
    /**
      * kafka配置信息
      */
    val kafkaParams1 = Map[String, Object](
        "bootstrap.servers" -> kafkaHost,
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> group,
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    /*
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
    */


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
        var kafkaConsumer: InputDStream[ConsumerRecord[String, String]] = null
        if (children > 0) {
            //如果保存过 offset，这里更好的做法，还应该和  kafka 上最小的 offset 做对比，不然会报 OutOfRange 的错误
            //获取kafka当前topic每个partition最早的offset
            val curEarlyKafkaOffsetMap = KafkaUtil.getInstance.getOneKafkaEarlyOffsetByTopicList(kafkaHost, topics1(0), group).get(topics1(0)).getOffsetList
            val curLastKafkaOffsetMap = KafkaUtil.getInstance.getOneKafkaOffsetByTopicList(kafkaHost, topics1(0), group).get(topics1(0)).getOffsetList
            for (i <- 0 until children) {
                val tp = new TopicPartition(topics1(0), i)
                val curEarlyKafkaOffset = curEarlyKafkaOffsetMap.get(i)
                val curLastKafkaOffset = curLastKafkaOffsetMap.get(i)
                val partitionZkOffset = zkClient.readData[String](s"${topicDirs.consumerOffsetDir}/${i}").toLong
                if (partitionZkOffset < curEarlyKafkaOffset) {
                    fromOffsets += (tp -> curEarlyKafkaOffset) //比最小偏移量还小，取当前实际最小值
                } else if (partitionZkOffset > curLastKafkaOffset) {
                    fromOffsets += (tp -> curLastKafkaOffset) //比最大偏移量还大，取当前实际最大值
                } else {
                    fromOffsets += (tp -> partitionZkOffset) //在正常范围内取当前记录值
                }
            }
            kafkaConsumer = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets))
        } else {
            //如果未保存，根据 kafkaParam 的配置使用最新或者最旧的 offset
            kafkaConsumer = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
        }
        kafkaConsumer
    }


    /**
      * 创建并更新持久化节点
      *
      * @param zkClient
      * @param path
      * @param data
      */
    def updatePersistentPath(zkClient: ZkClient, path: String, data: String) {
        try {
            zkClient.writeData(path, data);
        } catch {
            case e: ZkNoNodeException => {
                val parentDir = path.substring(0, path.lastIndexOf('/'))
                if (parentDir.length != 0) {
                    zkClient.createPersistent(parentDir, true)
                }
                zkClient.createPersistent(path, data);
            }
        }
    }


    /**
      * 创建并更新offset记录
      *
      * @param offsetRanges
      */
    def createOrUpdateOffset(offsetRanges: Array[OffsetRange]): Unit = {
        try {
            for (o <- offsetRanges) {
                val zkPath = s"${topicDirs.consumerOffsetDir}/${o.partition}"
                //将每个partition 的 offset 保存到 zookeeper
                updatePersistentPath(zkClient, zkPath, o.fromOffset.toString)
            }
        } catch {
            case e: Exception => {
                println("createOrUpdateOffset ERROR " + e)
            }
        }
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
        val wordCountsDdd = kafkaConsumer.transform { rdd =>
            offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //得到该 rdd 对应 kafka 的消息的 offset
            rdd
        }.map(_.value()).window(Seconds(timelong), Seconds(period))
        //调用DStream中的transform算子,可以进行数据转换
        wordCountsDdd.foreachRDD(rdd => {
            if (!rdd.isEmpty()) {
                println(System.currentTimeMillis() + "============" + name + "============")
                createOrUpdateOffset(offsetRanges) //创建并更新offset记录
                rdd.foreach(println)
            } else {
                println(System.currentTimeMillis() + "--no data input--")
            }
        })
    }


    def main(args: Array[String]) {
        val ssc = initStreamingContext("KafkaCount", "local[10]", 5)
        val kafkaConsumer1 = initKafkaConsumer(ssc, kafkaParams1, topics1)
        consumerCount(kafkaConsumer1, 10, 5, 5, "c1")
        ssc.start()
        ssc.awaitTermination()
    }
}
