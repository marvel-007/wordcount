package com.mocha.spark.test

/**
  * 流数据读取统计方式
  *
  * @author Yangxq
  * @version 2017/5/14 0:09
  */
object WordCountStreaming {
  /*def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("WordCountStreaming").setMaster("local[2]")
    //设置为本地运行模式，2个线程，一个监听，另一个处理数据
    val ssc = new StreamingContext(sparkConf, Seconds(20))
    // 时间间隔为20秒
    val lines = ssc.textFileStream("file:///home/hadoop/test-data/logfile")
    //这里采用本地文件，当然你也可以采用HDFS文件
    val wordCounts = lines.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }*/
}
