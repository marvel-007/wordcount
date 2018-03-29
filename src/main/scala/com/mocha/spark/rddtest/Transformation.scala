package com.mocha.spark.rddtest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD转换算法测试
  *
  * @author Yangxq
  * @version 2017/7/4 21:46
  */
object Transformation {

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
    * mapTest
    *
    * @param sc
    */
  def mapTest(sc: SparkContext) {
    val numsMap = sc.parallelize(1 to 10).map(_ * 2) //map适用于任何元素，对元素集合中的每个值使用函数进行操作
    numsMap.collect.foreach(println)
  }

  /**
    * filterTest
    *
    * @param sc
    */
  def filterTest(sc: SparkContext) {
    val numsFilter = sc.parallelize(1 to 10).filter(_ % 2 == 0) //对元素集合中的每个值使用函数进行判断过滤操作
    numsFilter.collect.foreach(println)
  }

  /**
    * flatMapTest
    *
    * @param sc
    */
  def flatMapTest(sc: SparkContext) {
    val bigData = Array("Scala Spark", "Java Hadoop", "Java Tachyon")
    val bigDataStr = sc.parallelize(bigData).flatMap(_.split(" ")) //对元素集合中的每个值使用函数进行操作,然后对函数操的结果合并成一个大的集合
    bigDataStr.collect.foreach(println)
  }


  def groupByKeyTest(sc: SparkContext) {
    val data = Array((100, "Spark"), (100, "Tachyon"), (70, "Hadoop"), (80, "Kafka"), (80, "Hbase"))
    val dataRDD = sc.parallelize(data).groupByKey() //按照相同的key对value进行分组，分组后的value是一个集合
    dataRDD.collect.foreach(println)
  }

  def reduceByKeyTest(sc: SparkContext) {
    val lines = sc.textFile("G:\\README.md")
    val wordsRDD = lines.flatMap(_.split(" ")).map(words => (words, 1)).reduceByKey(_ + _) //对相同key的value进行reduce函数操作，返回操作后的集合
    wordsRDD.collect.foreach(word => println(word._1, word._2))
  }

  def joinTest(sc: SparkContext) {
    val table1 = Array((1, "Spark"), (2, "Tachyon"), (3, "Hadoop"))
    val table2 = Array((1, "100"), (2, "95"), (3, "70"))
    val names = sc.parallelize(table1)
    val scores = sc.parallelize(table2)
    val nameAndScores = names.join(scores) //join 对相同key的集合做链接操作(1,(Spark,100))
    nameAndScores.collect.foreach(println)
  }

  def coGroupTest(sc: SparkContext) {
    val table1 = Array((1, "Spark"), (2, "Tachyon"), (3, "Hadoop"))
    val table2 = Array((1, "100"), (2, "90"), (3, "70"), (1, "110"), (2, "95"), (2, "60"))
    val names = sc.parallelize(table1)
    val scores = sc.parallelize(table2)
    val nameAndScores = names.cogroup(scores) //(1,(CompactBuffer(Spark),CompactBuffer(100, 110)))
    nameAndScores.collect.foreach(println)
  }

  /**
    * main方法
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sc = sparkContext("Transformation")

    //mapTest(sc)
    //filterTest(sc)
    //flatMapTest(sc)
    //groupByKeyTest(sc)
    reduceByKeyTest(sc)
    //joinTest(sc)
    //coGroupTest(sc)
    sc.stop()
  }


}
