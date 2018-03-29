package com.mocha.spark.sparksql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark Sql 操作UDF 和UDAF
  *
  * @author Yangxq
  * @version 2017/10/16 23:31
  */
object SparkSqlUdfTest {

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
      * UDF操作
      *
      * @param ss
      */
    def udfTest(sc: SparkContext, ss: SparkSession) = {
        val testData = Array("apark", "apark", "hadoop", "spark", "hadoop", "spark", "spark", "apark", "hadoop", "spark", "hadoop", "spark", "spark")
        val testDataRddRow = sc.parallelize(testData).map(x => Row(x))
        val scheam = StructType(
            List(
                StructField("name", StringType, true)
            )
        )
        val testDataDF = ss.createDataFrame(testDataRddRow, scheam) //注册为临时表
        testDataDF.createOrReplaceTempView("testDataTable")
        ss.udf.register("computerLength", (input: String) => input.length) //注册自定义函数UDF
        ss.sql("select name,computerLength(name) as length from testDataTable").show()
        while (true) {}
    }

    /**
      * 按照模板自定义UDAF类
      */
    class myCountUdaf extends UserDefinedAggregateFunction {
        //指定输入数据的类型
        override def inputSchema: StructType = StructType(List(StructField("input", StringType, true)))

        //在聚合操作的时所要处理数据的结果输出类型
        override def bufferSchema: StructType = StructType(List(StructField("count", IntegerType, true)))

        //指定UDAF函数计算后返回的结果类型
        override def dataType: DataType = IntegerType

        //确保一致性
        override def deterministic: Boolean = true

        //在聚合之前每组数据的初始化结果
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0
        }

        //在聚合时当有新值进来时，对分组聚合结果如何进行计算
        //本地的聚合操作，相当于hadoop mapreduce模型中的combiner
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            buffer(0) = buffer.getAs[Int](0) + 1
        }

        //在分布式节点进行Local Reduce 完成后进行全局级别的Merger操作
        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
            buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
        }

        //返回UDAF最后的返回结果
        override def evaluate(buffer: Row): Any = buffer.getAs[Int](0)
    }

    /**
      * UDAF操作:作用于数据集合
      *
      * @param ss
      */
    def udafTest(sc: SparkContext, ss: SparkSession) = {
        val testData = Array("spark", "spark", "hadoop", "spark", "hadoop", "spark", "spark", "spark", "hadoop", "spark", "hadoop", "spark", "spark")
        val testDataRddRow = sc.parallelize(testData).map(x => Row(x))
        val scheam = StructType(
            List(
                StructField("word", StringType, true)
            )
        )
        val testDataDF = ss.createDataFrame(testDataRddRow, scheam) //注册为临时表
        testDataDF.createOrReplaceTempView("testDataTable")
        ss.udf.register("computerLength", (input: String) => input.length) //注册自定义函数UDF
        ss.udf.register("wordcount", new myCountUdaf) //注册自定义函数UDAF
        ss.sql("select word,wordcount(word) as count, computerLength(word) as length from testDataTable group by word").show()
        while (true) {}
    }


    def main(args: Array[String]): Unit = {
        val appname = "SparkSqlUdfTest"
        //udfTest(sparkContext(appname), initSparkSession(appname))
        udafTest(sparkContext(appname), initSparkSession(appname))
    }
}