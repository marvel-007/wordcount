package com.mocha.spark.sparlml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession

/**
  * DecisiontreeTest
  *
  * @author Yangxq
  * @version 2017/12/11 22:19
  */
object KMeansTest {
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

    case class model_instance(features: Vector)

    /**
      * @param ss
      */
    def KMeansTest(ss: SparkSession) = {
        val data = ss.sparkContext.textFile("G:\\ml-study\\test-data\\iris.txt").map(line => {
            model_instance(Vectors.dense(line.split(",").filter(_.matches("\\d*(\\.?)\\d*")).map(_.toDouble)))
        })
        val df = ss.createDataFrame(data)
        df.show(false)
        val kmeansmodel = new KMeans().setK(3).setFeaturesCol("features").setPredictionCol("prediction").fit(df)
        val results = kmeansmodel.transform(df)
        results.collect().foreach(row => {
            println(row(0) + " is predicted as cluster " + row(1))
        })
        kmeansmodel.clusterCenters.foreach(center => {
            println("Clustering Center:" + center)
        })
        //集合内误差平方和
        val SquaredError = kmeansmodel.computeCost(df)
        println("Within Set Sum of Squared Error:" + SquaredError)
    }


    def main(args: Array[String]): Unit = {
        val sparkSession = initSparkSession("KMeansTest")
        KMeansTest(sparkSession)

    }
}
