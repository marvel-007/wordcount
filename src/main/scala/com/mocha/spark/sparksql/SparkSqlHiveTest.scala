package com.mocha.spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark Sql 操作Hive
  *
  * @author Yangxq
  * @version 2017/10/16 23:31
  */
object SparkSqlHiveTest {

    /**
      * 创建spark上下文
      *
      * @param appname
      * @return
      */
    def sparkContext(appname: String) = {
        val conf = new SparkConf().setAppName(appname).setMaster("local")
        Logger.getRootLogger.setLevel(Level.ERROR)
        val sc = new SparkContext(conf)
        sc
    }

    /**
      * 创建SparkSession上下文
      *
      * @return
      */
    def initSparkSession(appname: String): SparkSession = {
        Logger.getRootLogger.setLevel(Level.ERROR)
        val warehouseLocation = "/user/hive/warehouse/"
        val sparkSession = SparkSession.builder().appName(appname).config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
        sparkSession
    }


    /**
      * 读取Hive数据
      *
      * @param sparkSession
      */
    def readHiveTest(sparkSession: SparkSession) = {
        //sparkSession.sql("use default")
        //sparkSession.sql("SELECT * FROM default.logs_parquet")
        //val studentDF = sparkSession.table("mochalog.logs_accesslog")
        //val studentDF=sparkSession.sql("select name from default.logs_parquet")
/*        while (true){
            val studentDF=sparkSession.sql("select count(name) from default.logs_parquet")
            studentDF.show()
            Thread.sleep(3000)
        }*/
        val t1=System.currentTimeMillis()
        sparkSession.sql("select count(*) from mochalog.logs_accesslog").show()
        val t2=System.currentTimeMillis()
        println("查询语句总耗时：",t2-t1)
    }


    /**
      * 写入Hive数据
      *
      * @param sc
      * @param sparkSession
      */
    def writeHiveTest(sc: SparkContext, sparkSession: SparkSession) = {
        //val peopleList = List("{\"name\":\"yangxq\"}", "{\"name\":\"Andy111\",\"age\":203}", "{\"name\":\"Justin111\"}")
        val peopleList = List("{\"device_version\":\"x64\",\"request\":\"GET /tokr-web/static/js/common.js?v=3.20.9 HTTP/1.1\",\"upstream_addr\":\"192.168.40.51:8083\",\"body_bytes_sent\":\"21600\",\"browser_type\":\"Computer\",\"engine_name\":\"Mozilla\",\"http_user_agent\":\"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.108 Safari/537.36\",\"source_ip\":\"192.168.16.108\",\"remote_user\":\"-\",\"log_type\":\"nginx_accesslog\",\"device_name\":\"\",\"time_total\":24,\"collect_type\":\"accesslog\",\"system_version\":\"Win64\",\"host\":\"192.168.40.28\",\"host_ip_type\":\"192.168.40.28\",\"browser_version\":\"63.0.3239.108\",\"msg_md5\":\"Z/EhofxEmRlBX0j3XGuB0g==\",\"request_p\":\"GET /tokr-web/static/js/common.js\",\"app_id\":\"nginx_log\",\"request_q\":\"v=3.20.9 HTTP/1.1\",\"timestamp\":1520822934000,\"method\":\"GET\",\"system_name\":\"Windows NT 10.0\",\"message\":\"192.168.16.108-GET /tokr-web/static/js/common.js?v=3.20.9 HTTP/1.1-200-21600\",\"tags\":[\"nginx_log\",\"pc\"],\"app_name\":\"nginx28\",\"browser_name\":\"Chrome\",\"remote_addr_ip_type\":\"192.168.16.108\",\"http_referrer\":\"http://appdev.mochasoft.com.cn:8000/tokr-web/\",\"frontend_name\":\"\",\"engine_version\":\"5.0\",\"host_name\":\"appdev\",\"status\":\"200\"}")
        val peopleListRDD = sc.parallelize(peopleList)
        val peopleListDF = sparkSession.read.json(peopleListRDD)
        peopleListDF.createOrReplaceTempView("tempTable")
        //sparkSession.sql("DROP TABLE IF EXISTS logs_parquet")
        peopleListDF.show()
        //peopleListDF.write.mode(SaveMode.Append).saveAsTable("logs_parquet_1")
        //sparkSession.sql("DROP TABLE IF EXISTS logs_parquet")
        //sparkSession.sql("CREATE TABLE IF NOT EXISTS logs_parquet(name string,age int) STORED  AS PARQUET")
        /*  sparkSession.sql("INSERT INTO mochalog.logs_accesslog SELECT 'device_version' ,request ,'upstream_addr' ,body_bytes_sent,'browser_type' ," +
                             "'engine_name' ,http_user_agent ,source_ip ,remote_user ,log_type ,'device_name' ,time_total,collect_type ,'system_version' ,host ," +
                             "'browser_version' ,msg_md5 ,request_p ,app_id ,'request_q' ,timestamp,method ,'system_name' ,message ,tags," +
                             "app_name ,'browser_name' ,remote_addr_ip_type ,http_referrer ,frontend_name ,'engine_version' ,host_name ,status  FROM tempTable")*/
    }

    def main(args: Array[String]): Unit = {
        val appname = "SparkSqlHiveTest"
        //writeHiveTest(sparkContext(appname),initSparkSession(appname))
        readHiveTest(initSparkSession(appname))
    }
}