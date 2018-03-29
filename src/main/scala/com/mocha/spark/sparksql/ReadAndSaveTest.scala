package com.mocha.spark.sparksql

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
object ReadAndSaveTest {

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
      * 读取json 保存csv
      *
      * @param sparkSession
      */
    def test1(sparkSession: SparkSession) = {
        val df = sparkSession.read.json("G:\\spark-study\\spark-2.1.0-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
        df.select("name", "age").write.csv("G:\\spark-study\\newpeople.csv")
        df.rdd.saveAsTextFile("G:\\spark-study\\newpeople.txt")
    }

    /**
      * 读取json 增加保存csv
      *
      * @param sparkSession
      */
    def test2(sparkSession: SparkSession) = {
        val df = sparkSession.read.json("G:\\spark-study\\spark-2.1.0-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
        df.select("name", "age").write.mode(SaveMode.Append).option("header", true).csv("G:\\dataexport")
    }


    /**
      * parquet 数据格式测试
      *
      * @param sparkSession
      */
    def parquetReadTest(sparkSession: SparkSession) = {
        val parquetFileDF = sparkSession.read.parquet("G:\\spark-study\\spark-2.1.0-bin-hadoop2.7\\examples\\src\\main\\resources\\users.parquet")
        parquetFileDF.createOrReplaceTempView("parquetFile")
        val namesDF = sparkSession.sql("SELECT * FROM parquetFile")
        namesDF.foreach(attributes => println("Name: " + attributes.getAs("name") + "  favorite color:" + attributes.getAs("favorite_color") + "  favorite_numbers" + attributes.getAs("favorite_numbers")))
    }

    def parquetSaveTest(sparkSession: SparkSession) {
        val df = sparkSession.read.json("file:///G:\\spark-study\\spark-2.1.0-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
        df.select("name", "age").write.mode(SaveMode.Append).parquet("G:\\newpeople1.parquet")
    }


    def parquetSaveHdfsTest(sparkSession: SparkSession) {
        val df = sparkSession.read.json("file:///G:/spark-study/spark-2.1.0-bin-hadoop2.7/examples/src/main/resources/people.json")
        df.select("name", "age").write.mode(SaveMode.Append).parquet("hdfs://192.168.40.161:9000/user/test/newpeople.parquet")
    }

    def parquetReadHdfsTest(sparkSession: SparkSession) = {
        val parquetFileDF = sparkSession.read.parquet("hdfs://192.168.40.161:9000/user/hive/warehouse/mochalog.db/logs_accesslog")
        parquetFileDF.createOrReplaceTempView("parquetFile")
        val namesDF = sparkSession.sql("SELECT count(host) FROM parquetFile")
        namesDF.show()
    }

    /**
      * 综合练习
      *
      * @param sparkSession
      */
    def queryComTest(sc: SparkContext, sparkSession: SparkSession): Any = {
        //构建name  score 数据表
        val df = sparkSession.read.json("G:\\people.json")
        df.createOrReplaceTempView("peopleScore")
        val peopleScoreDF = sparkSession.sql("select * from peopleScore where score >90")
        peopleScoreDF.show()
        val peopleScoreRDD = peopleScoreDF.rdd.map(row => row.getAs("name").toString).collect()

        //构建name age 数据表
        val peopleList = List("{\"name\":\"Michael\",\"age\":20}", "{\"name\":\"Andy\",\"age\":17}", "{\"name\":\"Justin\",\"age\":19}")
        val peopleListRDD = sc.parallelize(peopleList)
        val peopleListDF = sparkSession.read.json(peopleListRDD)
        peopleListDF.createOrReplaceTempView("peopleAge")
        var sql_query = "select * from peopleAge where name in ("
        for (i <- 0 until peopleScoreRDD.length) {
            sql_query += "'" + peopleScoreRDD(i) + "'"
            if (i < peopleScoreRDD.length - 1) {
                sql_query += ","
            }
        }
        sql_query += ")"
        val peopleAgeDF = sparkSession.sql(sql_query)
        peopleAgeDF.show()
        val scoreRDD = peopleScoreDF.rdd.map(row => (row.getAs("name").toString, row.getAs("score").toString.toInt))
        val ageRDD = peopleAgeDF.rdd.map(row => (row.getAs("name").toString, row.getAs("age").toString.toInt))
        val resultRDD = scoreRDD.join(ageRDD)
        saveRDDtoJson(sparkSession, resultRDD)
    }

    /**
      * 将RDD转换为DataFrame
      *
      * @param sparkSession
      * @param someRDD
      */
    def saveRDDtoJson(sparkSession: SparkSession, someRDD: RDD[(String, (Int, Int))]) = {
        val fileName = ("name", "age", "score")
        val fields = List(StructField(fileName._1, StringType, nullable = true), StructField(fileName._2, IntegerType, nullable = true), StructField(fileName._3, IntegerType, nullable = true))
        val schema = StructType(fields)
        val rowRDD = someRDD.map(x => Row(x._1, x._2._2, x._2._1))
        val peopleDF = sparkSession.createDataFrame(rowRDD, schema)
        peopleDF.createOrReplaceTempView("peopleResult")
        val results = sparkSession.sql("select * from peopleResult")
        results.show()
    }


    def main(args: Array[String]): Unit = {
        val appname = "Rdd2DataFrameTest"
        //test1(sparkSession)
        //test2(initSparkSession(appname))
        //parquetReadTest(sparkSession)
        // parquetSaveTest(sparkSession)
        //queryComTest(sparkContext(appname), initSparkSession(appname))

        //parquetSaveHdfsTest( initSparkSession(appname))
        parquetReadHdfsTest(initSparkSession(appname))
    }


}
