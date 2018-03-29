package com.mocha.spark.rddtest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试文件创建RDD
  *
  * @author Yangxq
  * @version 2017/7/2 23:43
  */
object RDDByLocalFile {
  def main(args: Array[String]) {
    //local:本地运行，不依赖集群
    val conf = new SparkConf().setAppName("RDDByCollection").setMaster("local")

    //spark程序唯一入口，初始化核心组件，向Master注册程序
    val sc = new SparkContext(conf)
    val sum = sc.textFile("G:\\README.md").map(_.length).reduce(_ + _)
    println("lines total length = " + sum)
  }

}
