package com.mocha.spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


/**
  * Spark-sql  DataFrame 测试
  *
  * @author Yangxq
  * @version 2017/10/8 10:28
  */
object DataFrameTest {

    def main(args: Array[String]): Unit = {
        val sparkSession = initSparkSession("DataFrameTest")
        peopleDataQuery(sparkSession)
    }


    /**
      * 创建SparkSession上下文
      *
      * @return
      */
    def initSparkSession(appname: String): SparkSession = {
        Logger.getRootLogger.setLevel(Level.WARN)
        val sparkSession = SparkSession.builder().appName(appname).getOrCreate()
        sparkSession
    }


    def peopleDataQuery(sparkSession: SparkSession) = {
        val df = sparkSession.read.json("G:\\spark-study\\spark-2.1.0-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
        //df.show()
        //输出元数据
        //df.printSchema()
        //选择
        df.select("name","age").show()
        //多列选择
        //df.select(df("name"), df("age") + 10).show()
        //过滤
        df.filter(df("age") > 10).show()
        //分组聚合
        //df.groupBy("age").count().show()
        // 排序
        //df.sort(df("age").desc).show()
        //多列排序
        //df.sort(df("age").desc, df("name").asc).show()
        //对列进行重命名
        //df.select(df("name").as("username"), df("age")).show()
    }


}