package com.mocha.spark.sparksql

import org.apache.spark.sql.SparkSession

/**
  * Spark Sql agg
  *
  * @author Yangxq
  * @version 2017/11/16 22:31
  */
object SparkSqlWindowTest {

    /**
      * 创建SparkSession上下文
      *
      * @return
      */
    def initSparkSession(appname: String): SparkSession = {
        //Logger.getRootLogger.setLevel(Level.WARN)
        val warehouseLocation = "G:\\spark-study\\spark-2.1.0-bin-hadoop2.7"
        val sparkSession = SparkSession.builder().appName(appname).config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
        sparkSession
    }

    //topNGroup.txt
    /*  hadoop 100
      spark 88
      hadoop 86
      spark 95
      hadoop 78
      spark 78
      hadoop 88
      spark 83
      hadoop 100
      spark 77*/

    /**
      *
      * @param sparkSession
      */
    def sparkSqlWindowTest(sparkSession: SparkSession) = {
        sparkSession.sql("user test")
        sparkSession.sql("DROP TABLE IF EXISTS scores")
        sparkSession.sql("CREATE TABLE IF NOT EXISTS scores(name STRING,score INT)  ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '  LINES TERMINATED BY '\\n' ")
        sparkSession.sql("LOAD DATA  LOACL INPATH '/usr/data/topNGroup.txt' INTO TABLE scores")
        //导入linux本地文件数据到HIVE表中
        val restltDF = sparkSession.sql("SELECT name.score FROM " +
                " (SELECT name,score ,row_number() OVER (PARTITION BY name ORDER BY  score DESC ) rank  FROM scores) sub_scores" +
                " WHERE rank<=4 ")
        restltDF.show()
        sparkSession.sql("DROP TABLE IF EXISTS resultScores")
        restltDF.createOrReplaceTempView("scoreTempTable")
        sparkSession.sql("INSERT  INTO resultScores SELECT * FROM  scoreTempTable")
    }


    def main(args: Array[String]): Unit = {
        val appname = "SparkSqlWindowTest"
        sparkSqlWindowTest(initSparkSession(appname))
    }
}