package com.mocha.spark.test
import java.util.{HashMap => JavaHashmap}

import scala.collection.mutable.HashMap
/**
  * @author Yangxq
  * @version 2018/3/5 15:34
  */
object ScalaAppTest extends App{

    val javamap=new JavaHashmap[Any,Any]()
    javamap.put(1,"jack")
    javamap.put(2,"mary")
    println("省略main方法的可执行对象")
    for(key <- javamap.keySet().toArray){
        println(key+":"+javamap.get(key))
    }

    val scalaHashMap=new HashMap[String,String]
    scalaHashMap.put("Spark", "excellent")
    scalaHashMap.put("MapReduce", "good")
    scalaHashMap.foreach(e=>{
        println(e._1+":"+e._2)
    })
}
