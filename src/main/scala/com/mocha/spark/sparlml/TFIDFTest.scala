package com.mocha.spark.sparlml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

/**
  * Spark-ml  TFIDFTest
  *
  * @author Yangxq
  * @version 2017/12/8 10:28
  */
object TFIDFTest {

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
      * 在Spark ML库中，TF-IDF被分成两部分：TF (+hashing) 和 IDF。
      * *
      * TF: HashingTF 是一个Transformer，在文本处理中，接收词条的集合然后把这些集合转化成固定长度的特征向量。这个算法在哈希的同时会统计各个词条的词频。
      * *
      * IDF: IDF是一个Estimator，在一个数据集上应用它的fit（）方法，产生一个IDFModel。 该IDFModel 接收特征向量（由HashingTF产生），然后计算每一个词在文档中出现的频次。
      * IDF会减少那些在语料库中出现频率较高的词的权重。
      *
      * @param ss
      */
    def pipelinesTest2(ss: SparkSession) = {
        //构建训练数据集
        val sentenceData = ss.createDataFrame(Seq((0, "I heard about Spark and I love Spark"), (0, "I wish Java could use case classes"), (1, "Logistic regression models are neat"))).toDF("label", "sentence")
        //用tokenizer对句子进行分词
        val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
        val wordsData = tokenizer.transform(sentenceData)
        wordsData.show(false)
        //用HashingTF的transform()方法把句子哈希成特征向量，这里设置哈希表的桶数为2000
        val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(2000)
        val featurizedData = hashingTF.transform(wordsData)
        featurizedData.show(false)
        //用IDF来对单纯的词频特征向量进行修正，使其更能体现不同词汇对文本的区别能力,IDFModel是一个Transformer，调用它的transform()方法，即可得到每一个单词对应的TF-IDF度量值
        val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
        val idfModel = idf.fit(featurizedData)
        val rescaledData = idfModel.transform(featurizedData)
        //特征向量已经被其在语料库中出现的总次数进行了修正，通过TF-IDF得到的特征向量，在接下来可以被应用到相关的机器学习方法中
        rescaledData.select("features", "label").take(3).foreach(println)
    }

    def main(args: Array[String]): Unit = {
        val sparkSession = initSparkSession("DataFrameTest")
        pipelinesTest2(sparkSession)
    }
}