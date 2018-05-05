package com.mocha.spark.sparlml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer, Word2Vec}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Spark-ml  Pipelines
  *
  * @author Yangxq
  * @version 2017/12/8 10:28
  */
object PipelinesMl_1 {

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


    def pipelinesTest(ss: SparkSession) = {
        //构建训练数据集
        val trainingData = ss.createDataFrame(
            Seq(
                (0L, "a b c d e spark", 1.0),
                (1L, "b d", 0.0),
                (2L, "spark f g h", 1.0),
                (3L, "what ar you", 0.0),
                (4L, "i am spark", 1.0),
                (5L, "spark is good than hadoop", 1.0),
                (6L, "do you like me", 0.0),
                (7L, "i like spark verry much", 1.0),
                (8L, "hadoop mapreduce", 0.0),
                (9L, "hadoop is very old than spark", 1.0),
                (10L, "mapreduce is not good ", 0.0),
                (11L, "spark is good i like it", 1.0),
                (12L, "sparkis good", 1.0)
            )
        ).toDF("id", "text", "label")

        //定义 Pipeline 中的各个工作流阶段PipelineStage，包括转换器和评估器，具体的，包含tokenizer, hashingTF和lr三个步骤
        //Tokenizer和HashingTF是Transformers转换器，LogisticRegression（lr）是Estimator评估器
        val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
        val hashingTF = new HashingTF().setNumFeatures(1000).setInputCol(tokenizer.getOutputCol).setOutputCol("features")
        val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.01)

        //组织PipelineStages 并创建一个Pipeline
        val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, lr))
        val model = pipeline.fit(trainingData)

        //构建测试数据
        val test = ss.createDataFrame(Seq((1L, "spark i j k"), (2L, "l m n"), (3L, "spark a"), (4L, "apache hadoop"),(5L, "spard  probability is bad"))).toDF("id", "text")
        model.transform(test).select("id", "text", "probability", "prediction")
                .collect().foreach {
                    case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
                        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
                }
    }


    def main(args: Array[String]): Unit = {
        val sparkSession = initSparkSession("DataFrameTest")
        pipelinesTest(sparkSession)
    }
}