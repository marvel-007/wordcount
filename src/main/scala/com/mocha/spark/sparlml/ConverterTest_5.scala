package com.mocha.spark.sparlml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{IndexToString, OneHotEncoder, StringIndexer, VectorIndexer}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession

/**
  * Spark-ml  ConverterTest:StringIndexer,
  *
  * @author Yangxq
  * @version 2017/12/10 21:19
  */
object ConverterTest_5 {
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
      * StringIndexer转换器可以把一列类别型的特征（或标签）进行编码，使其数值化，索引的范围从0开始，该过程可以使得相应的特征索引化，
      * 使得某些无法接受类别型特征的算法可以使用，并提高诸如决策树等机器学习算法的效率
      *
      * @param ss
      */
    def StringIndexerTest(ss: SparkSession) = {
        val df1 = ss.createDataFrame(Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))).toDF("id", "category")
        val df2 = ss.createDataFrame(Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "d"))).toDF("id", "category")
        val model = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df1)
        val indexed1 = model.transform(df1)
        indexed1.show()
        //通过设置setHandleInvalid("skip")来忽略掉那些未出现的标签"d"
        val indexed2 = model.setHandleInvalid("skip").transform(df2)
        indexed2.show()
    }

    /**
      * 与StringIndexer相对应，IndexToString的作用是把标签索引的一列重新映射回原有的字符型标签。其主要使用场景一般都是和StringIndexer配合，
      * 先用StringIndexer将标签转化成标签索引，进行模型训练，然后在预测标签的时候再把标签索引转化成原有的字符标签。也可以另外定义其他的标签
      *
      * @param ss
      */
    def IndexToStringTest(ss: SparkSession) = {
        val df = ss.createDataFrame(Seq((0, "aadadasd"), (1, "badaewqeqqw"), (2, "cqweqadad"), (3, "adadqeqw"), (4, "aqefsdffg"), (5, "cfweqadd"))).toDF("id", "category")
        val model = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df)
        val indexed = model.transform(df)
        indexed.show()
        val converter = new IndexToString().setInputCol("categoryIndex").setOutputCol("originalCategory")
        val converted = converter.transform(indexed)
        converted.show()
    }

    /**
      * 独热编码（One-Hot Encoding） 是指把一列类别性特征（或称名词性特征，nominal/categorical features）映射成一系列的二元连续特征的过程，
      * 原有的类别性特征有几种可能取值，这一特征就会被映射成几个二元连续特征，每一个特征代表一种取值，若该样本表现出该特征，则取1，否则取0
      * One-Hot编码适合一些期望类别特征为连续特征的算法，比如说逻辑斯蒂回归等
      *
      * @param ss
      */
    def OneHotEncoderTest(ss: SparkSession) = {
        val df = ss.createDataFrame(Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"), (6, "d"), (7, "d"), (8, "d"), (9, "d"), (10, "e"), (11, "e"), (12, "e"), (13, "e"), (14, "e"))).toDF("id", "category")
        val indexer = new StringIndexer().setInputCol("category").setOutputCol("categoryIndex").fit(df)
        val indexed = indexer.transform(df)
        indexed.show()
        val encoder = new OneHotEncoder().setInputCol("categoryIndex").setOutputCol("categoryVec")
        val encoded = encoder.transform(indexed)
        encoded.show()
    }

    /**
      * StringIndexer是针对单个类别型特征进行转换，倘若所有特征都已经被组织在一个向量中，
      * 又想对其中某些单个分量进行处理时，Spark ML提供了VectorIndexer类来解决向量数据集中的类别性特征转换。
      *
      * @param ss
      */
    def VectorIndexerTest(ss: SparkSession) = {
        val data = Seq(Vectors.dense(-1.0, 1.0, 1.0), Vectors.dense(-1.0, 3.0, 1.0), Vectors.dense(0.0, 5.0, 1.0))
        val df = ss.createDataFrame(data.map(Tuple1.apply)).toDF("features")
        val vIndexerModel = new VectorIndexer().setInputCol("features").setOutputCol("indexed").setMaxCategories(2).fit(df)
        //可以通过VectorIndexerModel的categoryMaps成员来获得被转换的特征及其映射，这里可以看到共有两个特征被转换，分别是0号和2号
        val categoricalFeatures = vIndexerModel.categoryMaps.keys.toSet
        println(s"Chose ${categoricalFeatures.size} categorical features: " + categoricalFeatures.mkString(", "))
        val indexed = vIndexerModel.transform(df)
        indexed.show()
    }

    def main(args: Array[String]): Unit = {
        val sparkSession = initSparkSession("DataFrameTest")
        //StringIndexerTest(sparkSession)
        //IndexToStringTest(sparkSession)
        //OneHotEncoderTest(sparkSession)
        VectorIndexerTest(sparkSession)
    }
}
