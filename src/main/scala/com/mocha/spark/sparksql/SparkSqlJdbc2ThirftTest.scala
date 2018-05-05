package com.mocha.spark.sparksql

import java.sql.DriverManager

/**
  * Spark Sql 操作Jdbc2Thirft
  *
  * @author Yangxq
  * @version 2017/10/16 23:31
  */
object SparkSqlJdbc2ThirftTest {


    /**
      * Jdbc2Thirft操作
      *
      */
    def jdbc2ThirftTest(): Unit = {
        //add driver
        Class.forName("org.apache.hive.jdbc.HiveDriver")

        //get connection
        val connection = DriverManager.getConnection("jdbc:hive2://192.168.40.160:10010/mochalog", "hadoop", "")

        //get statement
        connection.prepareStatement("use mochalog").execute()
        val sql = "select status  from logs_accesslog where status = ?"
        val statement = connection.prepareStatement(sql)
        statement.setInt(1, 404)
        //get result*
        val resultSet = statement.executeQuery()
        while (resultSet.next()) {
            println(s"${resultSet.getString(1)},${resultSet.getString(2)}") //此处数据推荐保存为parquet
        }

        //close
        resultSet.close()
        statement.close()
        connection.close()
    }


    def jdbcCountTest(): Unit = {
        //add driver
        Class.forName("org.apache.hive.jdbc.HiveDriver")

        //get connection
        val connection = DriverManager.getConnection("jdbc:hive2://192.168.40.160:10000/mochalog", "hadoop", "")

        //get statement
        connection.prepareStatement("use mochalog").execute()
        val sql = "select count(host) from logs_accesslog"
        val statement = connection.prepareStatement(sql)
        //get result
        val resultSet = statement.executeQuery()
        while (resultSet.next()) {
            println(s"${resultSet.getString(1)}") //此处数据推荐保存为parquet
        }

        //close
        resultSet.close()
        statement.close()
        connection.close()
    }

    def main(args: Array[String]): Unit = {
        jdbc2ThirftTest()
        //jdbcCountTest()
    }
} 