package com.mocha.spark.rddtest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * TopN算法测试
  *
  * @author Yangxq
  * @version 2017/7/9 10:28
  */
object TopNTest {


  /**
    * 创建spark上下文
    *
    * @param appname
    * @return
    */
  def sparkContext(appname: String) = {
    val conf = new SparkConf().setAppName(appname).setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    sc
  }

  /**
    * 单列排序测试
    *
    * @param sc
    */
  def topNTest(sc: SparkContext): Unit = {
    val wordsRDD = sc.textFile("G:\\README.md").flatMap(_.split(" ")).map(words => (words, 1)).reduceByKey(_ + _).map(term => (term._2, term._1)).sortByKey(false).map(term => (term._2, term._1))
    wordsRDD.take(10).foreach(println)
  }

  /**
    * 分组排序测试
    *
    * @param sc
    */
  def topNByGroupTest(sc: SparkContext): Unit = {
    val lines = sc.textFile("G:\\spark-study\\test-data\\topNGroup.txt").map(line => (line.split(" ")(0), line.split(" ")(1))).groupByKey().sortByKey(true).map(x => (x._1, x._2.toList.sortWith(_ > _).take(10)))
    lines.take(10).foreach(println)
  }


  def main(args: Array[String]): Unit = {
    val sc = sparkContext("TopNTest")
    //topNTest(sc)
    topNByGroupTest(sc)
    sc.stop()
  }
}
