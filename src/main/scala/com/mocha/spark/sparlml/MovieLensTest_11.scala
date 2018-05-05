package com.mocha.spark.sparlml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.GaussianMixture
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/**
  *
  * 协同过滤电影推荐,ALS来建立推荐模型
  * @author Yangxq
  * @version 2017/12/11 22:19
  */
object MovieLensTest_11 {
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

    case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

    def parseRating(str: String): Rating = {
               val fields = str.split("::")
               assert(fields.size == 4)
             Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
    }

    /**
      * @param ss
      */
    def MovieLenKMeansTest(ss: SparkSession) = {
        val ratings = ss.sparkContext.textFile("file:///G:/ml-study/test-data/sample_movielens_ratings.txt").map(parseRating)
        val ratingsDF=ss.createDataFrame(ratings)
        ratingsDF.show()
        val Array(training, test) = ratingsDF.randomSplit(Array(0.8, 0.2))
        //构建了两个模型，一个是显性反馈，一个是隐性反馈
        val alsExplicit = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId"). setItemCol("movieId").setRatingCol("rating")
        val alsImplicit = new ALS().setMaxIter(5).setRegParam(0.01).setImplicitPrefs(true). setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
       /* 在 ML 中的实现有如下的参数:
        numBlocks 是用于并行化计算的用户和商品的分块个数 (默认为10)。
        rank 是模型中隐语义因子的个数（默认为10）。
        maxIter 是迭代的次数（默认为10）。
        regParam 是ALS的正则化参数（默认为1.0）。
        implicitPrefs 决定了是用显性反馈ALS的版本还是用适用隐性反馈数据集的版本（默认是false，即用显性反馈）。
        alpha 是一个针对于隐性反馈 ALS 版本的参数，这个参数决定了偏好行为强度的基准（默认为1.0）。
        nonnegative 决定是否对最小二乘法使用非负的限制（默认为false）。
        ​ 可以调整这些参数，不断优化结果，使均方差变小。比如：imaxIter越大，regParam越 小，均方差会越小，推荐结果较优。
        */
        val modelExplicit = alsExplicit.fit(training)
        val modelImplicit = alsImplicit.fit(training)
        val predictionsExplicit = modelExplicit.transform(test)
        val predictionsImplicit = modelImplicit.transform(test)
        predictionsExplicit.show()
        predictionsImplicit.show()
        val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating"). setPredictionCol("prediction")
        val rmseExplicit = evaluator.evaluate(predictionsExplicit)
        val rmseImplicit = evaluator.evaluate(predictionsImplicit)
        println(s"Explicit:Root-mean-square error = $rmseExplicit")
        println(s"Implicit:Root-mean-square error = $rmseImplicit")
    }


    def main(args: Array[String]): Unit = {
        val sparkSession = initSparkSession("KMeansTest")
        MovieLenKMeansTest(sparkSession)

    }
}
