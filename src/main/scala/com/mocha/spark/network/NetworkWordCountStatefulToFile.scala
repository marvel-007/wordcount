package com.mocha.spark.network

import com.mocha.spark.common.StreamingExamples
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 套接字流读取有状态统计方式并输出结果到文件
  * @author Yangxq
  * @version 2017/5/18 23:44
  */
object NetworkWordCountStatefulToFile {
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
    sc.checkpoint("file:///usr/local/spark/mycode/streaming/dstreamoutput/")
    //设置检查点，检查点具有容错机制
    val lines = sc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))
    val stateDstream = wordDstream.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    //下面是新增的语句，把DStream保存到文本文件中
    stateDstream.saveAsTextFiles("file:///usr/local/spark/mycode/streaming/dstreamoutput/output.txt")
    sc.start()
    sc.awaitTermination()
  }
}
