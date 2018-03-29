package com.mocha.spark.test

import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark main入口类
  * 通用文件读取单词统计方式
  *
  * @author Yangxq
  * @version 2017/5/2 22:55
  */
object WordCount {
    def main(args: Array[String]) {
        //创建spark配置对象SparkConf,设置运行时配置信息，程序名称
        val conf = new SparkConf().setAppName("word-count-app")
        //local:本地运行，不依赖集群
        //Logger.getRootLogger.setLevel(Level.WARN)
        //本地测试运行时设置
        //spark程序唯一入口，初始化核心组件，向Master注册程序
        val sc = new SparkContext(conf)
        //读取文件设置partitions为1，数据被RDD划分为多个partitions，每个partitions的数据属于一个Task处理
        //val lines = sc.textFile("/test/input/wordcount/data")
        val lines = sc.textFile("file:///home/hadoop/test-data/word.txt")
        //对初始的RDD进行transformation级别处理，例如 map、filter等高阶函数编程，进行数据计算
        //将每行数据拆分为单个单词,并合并所有行拆分结果为一个大单词集合
        val words = lines.flatMap(lines => lines.split(" "))
        //对每个单词计数设置1，即word=>(word,1)
        val pairs = words.map(words => (words, 1))
        //统计每个单词总数,相同单词次数累加
        val wordCounts = pairs.reduceByKey(_ + _).map(pair => (pair._2, pair._1)).sortByKey(false).map(pair => (pair._2, pair._1))
        //循环打印统计结果
        //wordCounts.foreach(wordNumPair => println(wordNumPair._1 + ":" + wordNumPair._2))
        wordCounts.collect.foreach(wordNumPair => println(wordNumPair._1 + ":" + wordNumPair._2)) //集群环境运行输出各个节点输出结果
        sc.stop()
    }
}
