package com.mocha.spark.sparlml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, DecisionTreeClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * DecisiontreeTest
  *
  * @author Yangxq
  * @version 2017/12/11 22:19
  */
object DecisiontreeTest {
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
      * @param ss
      */
    def DecisiontreeTest(ss: SparkSession) = {
        val data = ss.sparkContext.textFile("G:\\ml-study\\test-data\\iris.txt").map(_.split(","))
                .map(p => Iris(Vectors.dense(p(0).toDouble, p(1).toDouble, p(2).toDouble, p(3).toDouble), p(4).toString()))
        val dfall = ss.createDataFrame(data).createOrReplaceTempView("iris")
        val df = ss.sql("select * from iris")
        //分别获取标签列和特征列，进行索引，并进行了重命名。
        val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(df)
        val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(df)
        //这里我们设置一个labelConverter，目的是把预测的类别重新转化成字符型的。
        val labelConverter = new IndexToString().setInputCol("prediction").setOutputCol("predictedLabel").setLabels(labelIndexer.labels)
        //接下来，我们把数据集随机分成训练集和测试集，其中训练集占70%。
        val Array(trainingData, testData) = df.randomSplit(Array(0.7, 0.3))
        //训练决策树模型,这里我们可以通过setter的方法来设置决策树的参数，也可以用ParamMap来设置（具体的可以查看spark mllib的官网）。具体的可以设置的参数可以通过explainParams()来获取。
        val dtClassifier = new DecisionTreeClassifier().setLabelCol("indexedLabel").setFeaturesCol("indexedFeatures")
        // 在pipeline中进行设置
        val pipelinedClassifier = new Pipeline().setStages(Array(labelIndexer, featureIndexer, dtClassifier, labelConverter))
        //训练决策树模型
        val modelClassifier = pipelinedClassifier.fit(trainingData)
        //进行预测
        val predictionsClassifier = modelClassifier.transform(testData)
        //查看部分预测的结果
        predictionsClassifier.select("predictedLabel", "label", "features").show(20)
        //评估决策树分类模型
        val evaluatorClassifier = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("accuracy")
        val accuracy = evaluatorClassifier.evaluate(predictionsClassifier)
        println("Test Right = " + accuracy)
        println("Test Error = " + (1.0 - accuracy))
        val treeModelClassifier = modelClassifier.stages(2).asInstanceOf[DecisionTreeClassificationModel]
        println("Learned classification tree model:\n" + treeModelClassifier.toDebugString)
    }


    def main(args: Array[String]): Unit = {
        val sparkSession = initSparkSession("DecisiontreeTest")
        DecisiontreeTest(sparkSession)

    }
}
