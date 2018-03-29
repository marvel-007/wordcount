package com.mocha.spark.sparlml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.{SparkSession, functions}

/**
  * Spark-ml:分类器
  *
  * @author Yangxq
  * @version 2017/12/10 22:42
  */
object ClassifierTest {
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

    case class Iris(features: org.apache.spark.ml.linalg.Vector, label: String)

    /**
      * 逻辑斯蒂回归（logistic regression）是统计学习中的经典分类方法，属于对数线性模型。logistic回归的因变量可以是二分类的，也可以是多分类的
      * 二项逻辑斯蒂回归进行二分类分析
      *
      * @param ss
      */
    def logisticregressionTest(ss: SparkSession) = {
        val data = ss.sparkContext.textFile("G:\\ml-study\\test-data\\iris.txt").map(_.split(","))
                .map(p => Iris(Vectors.dense(p(0).toDouble, p(1).toDouble, p(2).toDouble, p(3).toDouble), p(4).toString()))
        val dfall = ss.createDataFrame(data)
        dfall.createOrReplaceTempView("iris")
        val df = ss.sql("select * from iris where label != 'Iris-setosa'")
        df.foreach(t => println(t(1) + ":" + t(0)))
        //分别获取标签列和特征列，进行索引，并进行了重命名
        val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df)
        val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").fit(df)
        //把数据集随机分成训练集和测试集，其中训练集占70%
        val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
        val lr = new LogisticRegression().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures").setMaxIter(10).setRegParam(0.3).setElasticNetParam(0.8)
        println("LogisticRegression parameters:\n" + lr.explainParams() + "\n")
        //设置一个labelConverter，目的是把预测的类别重新转化成字符型的
        val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
        //构建pipeline，设置stage，然后调用fit()来训练模型
        val lrPipelineModel = new Pipeline().setStages(Array(labelIndexer, featureIndexer, lr, labelConverter)).fit(trainingData)
        //利用训练得到的模型对测试集进行验证
        val lrPredictions = lrPipelineModel.transform(testData)
        lrPredictions.show(false)
        //创建一个MulticlassClassificationEvaluator实例，用setter方法把预测分类的列名和真实分类的列名进行设置；然后计算预测准确率和错误率
        val evaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction")
        val lrAccuracy = evaluator.evaluate(lrPredictions)
        println("Test Error = " + (1.0 - lrAccuracy))
        //调用它的stages来获取模型
        val lrModel = lrPipelineModel.stages(2).asInstanceOf[LogisticRegressionModel]
        println("Coefficients: " + lrModel.coefficients + "Intercept: " + lrModel.intercept + "numClasses: " + lrModel.numClasses + "numFeatures: " + lrModel.numFeatures)
        //模型评估-可以看到损失函数随着循环是逐渐变小的，损失函数越小，模型就越好
        val trainingSummary = lrModel.summary
        val objectiveHistory = trainingSummary.objectiveHistory
        objectiveHistory.foreach(loss => println(loss))
        val binarySummary = trainingSummary.asInstanceOf[BinaryLogisticRegressionSummary]
        //通过获取ROC，我们可以判断模型的好坏，areaUnderROC达到了0.9870967741935485，说明我们的分类器还是不错的
        //我们通过最大化fMeasure来选取最合适的阈值，其中fMeasure是一个综合了召回率和准确率的指标，通过最大化fMeasure，我们可以选取到用来分类的最合适的阈值
        println(s"areaUnderROC: ${binarySummary.areaUnderROC}")
        val fMeasure = binarySummary.fMeasureByThreshold
        val maxFMeasure = fMeasure.select(functions.max("F-Measure")).head().getDouble(0)
        println("maxFMeasure:" + maxFMeasure)
        fMeasure.show(false)
    }


    def main(args: Array[String]): Unit = {
        val sparkSession = initSparkSession("DataFrameTest")
        logisticregressionTest(sparkSession)

    }
}
