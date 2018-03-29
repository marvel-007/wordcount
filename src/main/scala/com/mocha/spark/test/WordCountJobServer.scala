package com.mocha.spark.test

import scala.util.Try

/*
*
  * spark main入口类
  * 通用文件读取单词统计方式
  *
  * @author Yangxq
  * @version 2017/5/2 22:55

object WordCountJobServer extends SparkJob {
    type JobData = Seq[String]
    type JobOutput = collection.Map[String, Long]

    def runJob(sc: SparkContext, runtime: JobEnvironment, data: JobData): JobOutput =
        sc.parallelize(data).countByValue

    def validate(sc: SparkContext, runtime: JobEnvironment, config: Config):
    JobData Or Every[ValidationProblem] = {
        Try(config.getString("input.string").split(" ").toSeq)
                .map(words => Good(words))
                .getOrElse(Bad(One(SingleProblem("No input.string param"))))
    }
}*/
