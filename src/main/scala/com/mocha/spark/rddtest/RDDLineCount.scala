package com.mocha.spark.rddtest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 每行相同单词数量统计
  * @author Yangxq
  * @version 2017/7/4 22:04
  */
object RDDLineCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDLineCount").setMaster("local")
    val sc = new SparkContext(conf)
    val txtlines = sc.textFile("G:\\spark-study\\test.txt").map(line => (line, 1)).reduceByKey(_ + _)
    txtlines.collect().foreach(pair => println(pair._1 + "--" + pair._2))
  }
}
