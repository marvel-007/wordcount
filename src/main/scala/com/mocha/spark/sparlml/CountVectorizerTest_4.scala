package com.mocha.spark.sparlml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession

/**
  * Spark-ml  CountVectorizer
  *
  * @author Yangxq
  * @version 2017/12/8 10:28
  */
object CountVectorizerTest_4 {

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
      * CountVectorizer旨在通过计数来将一个文档转换为向量。当不存在先验字典时，Countvectorizer作为Estimator提取词汇进行训练，
      * 并生成一个CountVectorizerModel用于存储相应的词汇向量空间。该模型产生文档关于词语的稀疏表示，其表示可以传递给其他算法，例如LDA。
      *
      * @param ss
      */
    def CountVectorizerTest(ss: SparkSession) = {
        val df = ss.createDataFrame(Seq((0, Array("a", "b", "c")), (1, Array("a", "b", "b", "c", "a")))).toDF("id", "words")
        //通过CountVectorizer设定超参数，训练一个CountVectorizerModel，这里设定词汇表的最大量为3，设定词汇表中的词至少要在2个文档中出现过，以过滤那些偶然出现的词汇
        val cvModel = new CountVectorizer().setInputCol("words").setOutputCol("features").setVocabSize(3).setMinDF(2).fit(df)
        cvModel.transform(df).show(false)

        //和其他Transformer不同，CountVectorizerModel可以通过指定一个先验词汇表来直接生成，如以下例子，直接指定词汇表的成员是“a”，“b”，“c”三个词
        val cvm = new CountVectorizerModel(Array("a", "b", "c")).setInputCol("words").setOutputCol("features")
        cvm.transform(df).show(false)
    }

    def main(args: Array[String]): Unit = {
        val sparkSession = initSparkSession("DataFrameTest")
        CountVectorizerTest(sparkSession)
    }
}