package com.mocha.spark.rddtest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 二次排序测试
  *
  * @author Yangxq
  * @version 2017/7/9 0:18
  */
object SecondarySortTest {

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


  def secondarySort(sc: SparkContext) {
    val pairSortKey = sc.textFile("G:\\secondSort.txt").map(line => (new SecondarySortKey(line.split(" ")(0).toInt, line.split(" ")(1).toInt), line))
    val sortedResult = pairSortKey.sortByKey(false).map(_._2)
    sortedResult.collect.foreach(println)
  }

  def main(args: Array[String]): Unit = {
    val sc = sparkContext("SecondarySortTest")
    secondarySort(sc)
    sc.stop()
  }
}
