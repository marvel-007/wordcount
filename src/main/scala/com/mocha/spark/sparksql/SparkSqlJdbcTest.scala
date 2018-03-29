package com.mocha.spark.sparksql

import java.util.Properties

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark Sql 操作JDBC
  *
  * @author Yangxq
  * @version 2017/10/16 23:31
  */
object SparkSqlJdbcTest {

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
        //Logger.getRootLogger.setLevel(Level.WARN)
        val sparkSession = SparkSession.builder().appName(appname).getOrCreate()
        sparkSession
    }


    /**
      * 读取mysql数据
      *
      * @param sparkSession
      */
    def readJdbcTest(sparkSession: SparkSession) = {
        val properties = Map(
            "driver" -> "com.mysql.jdbc.Driver",
            "url" -> "jdbc:mysql://192.168.31.212:3306/test",
            "dbtable" -> "people",
            "user" -> "root",
            "password" -> "123456"
        )
        val jdbcDF = sparkSession.read.format("jdbc").options(properties).load()
        //jdbcDF.filter(jdbcDF("age") > 19).select("name", "age").groupBy("age").count().show()
        jdbcDF.show()
    }


    /**
      * 写入MySQL数据
      *
      * @param sc
      * @param sparkSession
      */
    def writeJdbcTest(sc: SparkContext, sparkSession: SparkSession) = {
        //通过并行化创建RDD
        val personRDD = sc.parallelize(Array("4 tom 15 1355555", "5 jerry 13 13666666", "6 kitty 16 1377777")).map(_.split(" "))
        //通过StructType直接指定每个字段的schema
        val schema = StructType(
            List(
                StructField("id", IntegerType, true),
                StructField("name", StringType, true),
                StructField("age", IntegerType, true),
                StructField("telephone", StringType, true)
            )
        )
        //将RDD映射到rowRDD
        val rowRDD = personRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).toInt, p(3).trim))
        //将schema信息应用到rowRDD上
        val personDataFrame = sparkSession.createDataFrame(rowRDD, schema)
        //创建Properties存储数据库相关属性
        val prop = new Properties()
        prop.put("user", "root")
        prop.put("password", "123456")
        prop.put("driver", "com.mysql.jdbc.Driver")
        //将数据追加到数据库
        personDataFrame.write.mode(SaveMode.Append).jdbc("jdbc:mysql://192.168.31.212:3306/test", "people", prop)
        sc.stop()
    }

    def main(args: Array[String]): Unit = {
        val appname = "SparkSqlJdbcTest"
        readJdbcTest(initSparkSession(appname))
        //writeJdbcTest(sparkContext(appname: String), initSparkSession(appname))
    }
}