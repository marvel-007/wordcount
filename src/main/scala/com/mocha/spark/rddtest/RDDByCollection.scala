package com.mocha.spark.rddtest

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 测试集合创建RDD
  *
  * @author Yangxq
  * @version 2017/7/2 23:43
  */
object RDDByCollection {
  def main(args: Array[String]) {
    //local:本地运行，不依赖集群
    val conf = new SparkConf().setAppName("RDDByCollection").setMaster("local")

    //spark程序唯一入口，初始化核心组件，向Master注册程序
    val sc = new SparkContext(conf)
    val numbers = 1 to 100
    val sum = sc.parallelize(numbers).reduce(_ + _)
    println("1+2+3+.....+99+100 = " + sum)
  }

}
