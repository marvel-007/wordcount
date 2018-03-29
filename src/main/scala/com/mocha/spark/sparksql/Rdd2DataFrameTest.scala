package com.mocha.spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * RDD 转换  DataFrame 测试
  *
  * @author Yangxq
  * @version 2017/10/8 10:28
  */
object Rdd2DataFrameTest {


    /**
      * 创建spark上下文
      *
      * @param appname
      * @return
      */
    def sparkContext(appname: String) = {
        val conf = new SparkConf().setAppName(appname).setMaster("local")
        Logger.getRootLogger.setLevel(Level.WARN)
        val sc = new SparkContext(conf)
        sc
    }

    /**
      * 创建SparkSession上下文
      *
      * @return
      */
    def initSparkSession(appname: String): SparkSession = {
        //Logger.getRootLogger.setLevel(Level.WARN)
        val sparkSession = SparkSession.builder().appName(appname).getOrCreate()
        sparkSession
    }


    /**
      * 通过反射转换-提前知道数据结构
      *
      * @param sc
      */
    def getDFByReflection(sc: SparkContext, sparkSession: SparkSession) = {
        val personRDD = sc.textFile("G:\\spark-study\\spark-2.1.0-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt").map(_.split(",")).map(x => Person(x(0), x(1).trim.toInt))
        val personsDF = sparkSession.createDataFrame(personRDD)
        personsDF.createOrReplaceTempView("persons")
        val results = sparkSession.sql("select name,age from persons where age > 20")
        //results.show()
        results.foreach(row => println("name:" + row(0) + "   age:" + row(1)))
    }


    /**
      * 通过编程方式转换-不知道数据结构
      *
      * @param sc
      */
    def getDFByPrograme(sc: SparkContext, sparkSession: SparkSession) = {
        val peopleRDD = sc.textFile("G:\\spark-study\\spark-2.1.0-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt")
        val schemaString = "name age"
        val fields = schemaString.split(" ").map(field => StructField(field, StringType, nullable = true))
        val schema = StructType(fields)
        val rowRDD = peopleRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))
        val peopleDF = sparkSession.createDataFrame(rowRDD, schema)
        peopleDF.createOrReplaceTempView("people")
        val results = sparkSession.sql("select name,age from people where age >20")
        //results.show()
        results.foreach(row => println("name:" + row(0) + "    age:" + row(1)))
    }


    def main(args: Array[String]): Unit = {
        val appname = "Rdd2DataFrameTest"
        //getDFByReflection(sparkContext(appname), initSparkSession(appname))
        getDFByPrograme(sparkContext(appname), initSparkSession(appname))
    }


}