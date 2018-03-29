package com.mocha.spark.rddtest

import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * RDD行动操作测试
  *
  * @author Yangxq
  * @version 2017/7/4 21:46
  */
object Action {

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
    * reduceTest
    *
    * @param sc
    */
  def reduceTest(sc: SparkContext) {
    val sum = sc.parallelize(1 to 100).reduce(_ + _) //reduce对元素集合中的每个值使用函数进行操作,聚合数据集中的元素
    println("1+2+3+...+99+100=" + sum)
  }

  /**
    * collectTest
    *
    * @param sc
    */
  def collectTest(sc: SparkContext) {
    val results = sc.parallelize(1 to 100).map(_ * 2)
    results.collect.foreach(println) //collect以数组的形式返回数据集中的所有元素
  }

  /**
    * countTest
    *
    * @param sc
    */
  def countTest(sc: SparkContext) {
    val results = sc.parallelize(1 to 100).count() //count返回数据集中的元素个数
    println("results=" + results)
  }

  /**
    * takeTest
    *
    * @param sc
    */
  def takeTest(sc: SparkContext) {
    val results = sc.parallelize(1 to 100).take(10) //take以数组的形式返回数据集中的前n个元素
    results.foreach(println)
  }

  /**
    * countByKeyTest
    *
    * @param sc
    */
  def countByKeyTest(sc: SparkContext) {
    val data = Array((1, "100"), (2, "90"), (1, "70"), (2, "110"), (1, "95"), (3, "60"))
    val scores = sc.parallelize(data)
    val countScores = scores.countByKey()
    countScores.foreach(println)
  }

  /**
    * countByValueTest
    * ((3,60),1)
    * ((1,100),2)
    * ((1,70),1)
    * ((2,90),2)
    *
    * @param sc
    */
  def countByValueTest(sc: SparkContext) {
    val data = Array((1, "100"), (2, "90"), (1, "70"), (1, "100"), (2, "90"), (3, "60"))
    val scores = sc.parallelize(data)
    val countScores = scores.countByValue()
    countScores.foreach(println)
  }


  /**
    * saveAsTextFileTest
    *
    * @param sc
    */
  def saveAsTextFileTest(sc: SparkContext) {
    val lines = sc.textFile("G:\\README.md")
    val wordsRDD = lines.flatMap(_.split(" ")).map(words => (words, 1)).reduceByKey(_ + _)
    wordsRDD.saveAsTextFile("G:\\saveAsTextFileTest.txt")
  }


  /**
    * 链路数据统计
    *
    * @param sc
    */
  def hostDataCountTest(sc: SparkContext) {
    val lines = sc.textFile("file:///G:/logdata.txt")
    val wordsRDD1 = lines.map(_.split(",")).map(x=>(x(0),x(3)+"#"+x(4)+"#host"))
    val wordsRDD2 = lines.map(_.split(",")).map(x=>(x(1),x(3)+"#"+x(4)))
    val wordsRDD=wordsRDD1.union(wordsRDD2)
    var resultRDD=wordsRDD.groupByKey().map(x => {
      var nodeName=""
      var tempTime = ""
      for (info <- x._2.toList) {
        if(info.contains("host")){
          val node = info.split("#")(0)
          val t1 = info.split("#")(1)
          if(tempTime.isEmpty||t1<tempTime){
            tempTime=t1
            nodeName=node
          }
          println(info)
        }
      }
      println(tempTime,nodeName+"#"+x._1)
      ( tempTime,nodeName+"#"+x._1)
    }).sortByKey().map(x=>{
    /*  (1,n1#40.101)
      (2,n2#40.201)
      (3,n3#40.202)*/
      val host=x._2.split("#")(1)
      val node=x._2.split("#")(0)
      (host,node)
    }).map(x=>(1,x._1+":"+x._2)).groupByKey()

    resultRDD.foreach(x=>println(x._2.toList))
  }




  /**
    * 链路数据统计
    *
    * @param sc
    */
  def hostDataCountTest1(sc: SparkContext) {
    val lines = sc.textFile("file:///G:/logdata.txt")
    val wordsRDD1 = lines.map(_.split(",")).map(x=>(x(0),x(3)+"#"+x(4)+"#host"))
    val wordsRDD2 = lines.map(_.split(",")).map(x=>(x(1),x(3)+"#"+x(4)))
    val wordsRDD=wordsRDD1.union(wordsRDD2)
    val resultRDD=wordsRDD.groupByKey().filter(_._2.toString().contains("host")).map(x => {
      var nodeName=""
      var tempTime = ""
      for (info <- x._2.toList) {
        if(info.contains("host")){
          val node = info.split("#")(0)
          val t1 = info.split("#")(1)
          if(tempTime.isEmpty||t1.toLong<tempTime.toLong){
            tempTime=t1
            nodeName=node
          }
        }
      }
      ( tempTime.toLong,nodeName+"#"+x._1)
    }).sortByKey().map(x=>{
      val host=x._2.split("#")(1)
      val node=x._2.split("#")(0)
      (host,node)
    }).map(x=>(1,x._1+":"+x._2)).groupByKey()

    resultRDD.foreach(x=>println(x._2.toList))
  }


  /**
    * main方法
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sc = sparkContext("Transformation")
    //reduceTest(sc)
    //collectTest(sc)
    //countTest(sc)
    //takeTest(sc)
    //countByKeyTest(sc)
    //countByValueTest(sc)
    //saveAsTextFileTest(sc)
    hostDataCountTest1(sc)
    sc.stop()
  }


}
