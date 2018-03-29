package com.mocha.spark.sparlml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.ChiSqSelector
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Spark-ml  ConverterTest
  *
  * @author Yangxq
  * @version 2017/12/10 21:19
  */
object ChiSqSelectorTest {
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
      * 特征选择（Feature Selection）指的是在特征向量中选择出那些“优秀”的特征，组成新的、更“精简”的特征向量的过程。
      * 它在高维数据分析中十分常用，可以剔除掉“冗余”和“无关”的特征，提升学习器的性能
      *
      * @param ss
      */
    def ConverterTest(ss: SparkSession) = {
        val df = ss.createDataFrame(Seq((1, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1), (2, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0), (3, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0))).toDF("id", "features", "label")
        df.show()
        //用卡方选择进行特征选择器的训练，为了观察地更明显，我们设置只选择和标签关联性最强的一个特征（可以通过setNumTopFeatures(..)方法进行设置）
        val selector_model = new ChiSqSelector().setNumTopFeatures(1).setFeaturesCol("features").setLabelCol("label").setOutputCol("selected-feature").fit(df)
        //用训练出的模型对原数据集进行处理，可以看见，第三列特征被选出作为最有用的特征列
        val result = selector_model.transform(df)
        result.show(false)
    }


    def main(args: Array[String]): Unit = {
        val sparkSession = initSparkSession("DataFrameTest")
        ConverterTest(sparkSession)

    }
}
