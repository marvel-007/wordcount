package com.mocha.spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * DataFrame 数据读取与保存
  *
  * @author Yangxq
  * @version 2017/10/9 23:26
  */
object HdfsReadAndSaveTest {

    /**
      * 创建spark上下文
      *
      * @param appname
      * @return
      */
    def sparkContext(appname: String) = {
        val conf = new SparkConf().setAppName(appname).setMaster("local")
        //Logger.getRootLogger.setLevel(Level.WARN)
        val sc = new SparkContext(conf)
        sc
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



    def parquetSaveHdfsTest(sparkSession: SparkSession) {
        val df = sparkSession.read.json("file:///G:/logdata.txt")
        df.write.mode(SaveMode.Append).parquet("hdfs://192.168.40.161:9000/user/hive/warehouse/logs_parquet")
    }

    def parquetReadHdfsTest(sparkSession: SparkSession) = {
        val parquetFileDF = sparkSession.read.parquet("hdfs://192.168.40.161:9000/user/hive/warehouse/logs_parquet")
        parquetFileDF.createOrReplaceTempView("parquetFile")
        val namesDF = sparkSession.sql("SELECT * FROM parquetFile")
        namesDF.show()
       // namesDF.foreach(attributes => println("Name: " + attributes.getAs("name") + "  favorite color:" + attributes.getAs("favorite_color") + "  favorite_numbers" + attributes.getAs("favorite_numbers")))
    }



    def main(args: Array[String]): Unit = {
        val appname = "Rdd2DataFrameTest"
        parquetSaveHdfsTest( initSparkSession(appname))
        parquetReadHdfsTest(initSparkSession(appname))
    }


}
