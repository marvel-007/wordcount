package com.mocha.spark.rddtest

import com.sun.corba.se.spi.orbutil.fsm.Guard.Result
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark -hbase 写入数据
  *
  * @author Yangxq
  * @version 2017/12/25 18:07
  */
object SparkWriteHBase {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("SparkWriteHBase").setMaster("local")
        val sc = new SparkContext(sparkConf)
        val tablename = "student"
        sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tablename)

        val job = new Job(sc.hadoopConfiguration)
        job.setOutputKeyClass(classOf[ImmutableBytesWritable])
        job.setOutputValueClass(classOf[Result])
        job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

        val indataRDD = sc.makeRDD(Array("3,Rongcheng,M,26", "4,Guanhua,M,27")) //构建两行记录
        val rdd = indataRDD.map(_.split(',')).map { arr => {
            val put = new Put(Bytes.toBytes(arr(0))) //行健的值
            put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(arr(1))) //info:name列的值
            put.add(Bytes.toBytes("info"), Bytes.toBytes("gender"), Bytes.toBytes(arr(2))) //info:gender列的值
            put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(arr(3).toInt)) //info:age列的值
            (new ImmutableBytesWritable, put)
        }
        }
        rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
    }
}
