package com.mocha.spark.sparlml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession

/**
  * Spark-ml  word2VecTest
  *
  * @author Yangxq
  * @version 2017/12/8 10:28
  */
object Word2VecTest_3 {

    /**
      * 创建SparkSession上下文
      *
      * @return
      */
    def initSparkSession(appname: String): SparkSession = {
        Logger.getRootLogger.setLevel(Level.WARN)
        val sparkSession = SparkSession.builder().master("local").appName(appname).getOrCreate()
        sparkSession
    }


    /**
      * Word2Vec 是一种著名的 词嵌入（Word Embedding） 方法，它可以计算每个单词在其给定语料库环境下的
      * 分布式词向量（Distributed Representation，亦直接被称为词向量）。词向量表示可以在一定程度上刻画每个单词的语义
      *
      * @param ss
      */
    def word2VecTest(ss: SparkSession) = {
        val documentDF = ss.createDataFrame(
                            Seq("Hi I heard about Spark".split(" "), "I wish Java could use case classes".split(" "), "Logistic regression models are neat".split(" "))
                    .map(Tuple1.apply)).toDF("text")
        documentDF.show(false)
        val word2Vec = new Word2Vec().setInputCol("text").setOutputCol("result").setVectorSize(3).setMinCount(0)
        //​利用Word2VecModel把文档转变成特征向量
        val model = word2Vec.fit(documentDF)
        //文档被转变为了一个3维的特征向量，这些特征向量就可以被应用到相关的机器学习方法中
        val result = model.transform(documentDF)
        result.show(false)
    }

    def main(args: Array[String]): Unit = {
        val sparkSession = initSparkSession("DataFrameTest")
        word2VecTest(sparkSession)
    }
}