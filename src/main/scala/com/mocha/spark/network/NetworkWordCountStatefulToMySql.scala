package com.mocha.spark.network

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.mocha.spark.common.StreamingExamples
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 套接字流读取有状态统计方式并输出结果到数据库
  *
  * @author Yangxq
  * @version 2017/5/18 23:46
  */
object NetworkWordCountStatefulToMySql {

  def main(args: Array[String]) {
    //定义状态更新函数
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    StreamingExamples.setStreamingLogLevels()
    //设置log4j日志级别
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCountStateful")
    val sc = new StreamingContext(conf, Seconds(5))
    sc.checkpoint("file:///usr/local/spark/mycode/streaming/dstreamoutput/")
    //设置检查点，检查点具有容错机制
    val lines = sc.socketTextStream("localhost", 9999)
    val stateDstream = lines.flatMap(_.split(" ")).map(x => (x, 1)).updateStateByKey[Int](updateFunc)
    stateDstream.print()
    //下面是新增的语句，把DStream保存到MySQL数据库中
    saveDstreamToMySql(stateDstream)
    sc.start()
    sc.awaitTermination()
  }


  /**
    * 把DStream保存到MySQL数据库中
    *
    * @param stateDstream
    */
  def saveDstreamToMySql(stateDstream: DStream[(String, Int)]): Unit = {
    stateDstream.foreachRDD(rdd => {
      //内部函数
      def func(records: Iterator[(String, Int)]) {
        var conn: Connection = null
        var stmt: PreparedStatement = null
        try {
          val url = "jdbc:mysql://localhost:3306/spark"
          val user = "root"
          val password = "123456"
          conn = DriverManager.getConnection(url, user, password)
          records.foreach(p => {
            val sql = "insert into wordcount(word,count) values (?,?)"
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, p._1.trim)
            stmt.setInt(2, p._2.toInt)
            stmt.executeUpdate()
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (stmt != null) {
            stmt.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      }

      val repartitionedRDD = rdd.repartition(3)
      repartitionedRDD.foreachPartition(func)
    })
  }
}
