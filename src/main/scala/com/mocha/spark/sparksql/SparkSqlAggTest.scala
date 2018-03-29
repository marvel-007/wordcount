package com.mocha.spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark Sql agg
  *
  * @author Yangxq
  * @version 2017/11/16 22:31
  */
object SparkSqlAggTest {

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
      *
      *
      * @param sparkSession
      */
    def sparkSqlAggTest(sc: SparkContext, sparkSession: SparkSession) = {
        val userData = Array(
            "2016-03-27,001,http://spark.apache.org,1000",
            "2016-03-27,001,http://hadoop.apache.org,1001",
            "2016-03-27,002,http://fink.apache.org,1002",
            "2016-03-28,003,http://kafka.apache.org,1020",
            "2016-03-28,004,http://spark.apache.org,1010",
            "2016-03-28,002,http://hive.apache.org,1200",
            "2016-03-28,001,http://parquet.apache.org,1500",
            "2016-03-28,001,http://spark.apache.org,1800"
        )
        val userDataRdd = sc.parallelize(userData)
        val userDataSchema = StructType(
            List(
                StructField("time", StringType, true),
                StructField("id", StringType, true),
                StructField("url", StringType, true),
                StructField("amount", IntegerType, true)
            )
        )
        val userDataRow = userDataRdd.map(_.split(",")).map(x => Row(x(0), x(1), x(2), x(3).toInt))
        val userDataDF = sparkSession.createDataFrame(userDataRow, userDataSchema)
        //要使用spark sql的内置函数必须导入sparkSession的隐式转换
        import sparkSession.implicits._
        val aggDataDF1 = userDataDF.groupBy("time").agg('time, countDistinct('id))
        aggDataDF1.rdd.map(row => Row(row(1), row(2))).collect.foreach(println)

        val aggDataDF2 = userDataDF.groupBy("time").agg('time, sum('amount)).show()
    }


    def main(args: Array[String]): Unit = {
        val appname = "SparkSqlAggTest"
        sparkSqlAggTest(sparkContext(appname: String), initSparkSession(appname))
    }
}