package com.mocha.spark.network

import com.mocha.spark.common.StreamingExamples
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 套接字流读取有状态统计方式
  *
  * @author Yangxq
  * @version 2017/5/18 22:26
  */
object NetworkWordCountStateful {
  def main(args: Array[String]) {
    //定义状态更新函数
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    StreamingExamples.setStreamingLogLevels()
    //设置log4j日志级别
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCountStateful")
    val sc = new StreamingContext(conf, Seconds(5))
    sc.checkpoint("file:///usr/local/spark/mycode/streaming/stateful/")
    //设置检查点，检查点具有容错机制
    val lines = sc.socketTextStream("localhost", 9999)
    val stateDstream = lines.flatMap(_.split(" ")).map(x => (x, 1)).updateStateByKey[Int](updateFunc)
    stateDstream.print()
    sc.start()
    sc.awaitTermination()
  }
}
