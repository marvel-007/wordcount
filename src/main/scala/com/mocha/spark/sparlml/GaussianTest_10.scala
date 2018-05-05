package com.mocha.spark.sparlml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.{GaussianMixture, KMeans}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession

/**
  * Gaussian  Test
  * 高斯混合模型（Gaussian Mixture Model, GMM） 是一种概率式的聚类方法，属于生成式模型
  *
  * @author Yangxq
  * @version 2017/12/11 22:19
  */
object GaussianTest_10 {
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

    case class model_instance (features: Vector)

    /**
      * @param ss
      */
    def GaussianTest(ss: SparkSession) = {
        val data = ss.sparkContext.textFile("file:///G:/ml-study/test-data/iris.txt").map(line => {
            model_instance(Vectors.dense(line.split(",").filter(_.matches("\\d*(\\.?)\\d*")).map(_.toDouble)))
        })
        val df = ss.createDataFrame(data)
        df.show(false)
        val gmModel = new GaussianMixture().setK(3).setPredictionCol("Prediction").setProbabilityCol("Probability").fit(df)
        val result = gmModel.transform(df)
        result.take(100).foreach(println)

        for ( i <- 0 until gmModel.getK) {
             println("Component %d : weight is %f \n mu vector is %s \n sigma matrix is %s" format(i, gmModel.weights(i), gmModel.gaussians(i).mean, gmModel.gaussians(i).cov))
        }
    }


    def main(args: Array[String]): Unit = {
        val sparkSession = initSparkSession("KMeansTest")
        GaussianTest(sparkSession)
    }
}
