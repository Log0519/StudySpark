package com.log.spark.core.dependence

import org.apache.spark.{SparkConf, SparkContext}


object spark01_Dep_spark {
  def main(args: Array[String]): Unit = {

    //TODO 建立和spark的链接

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //TODO 执行业务操作
    //toDeBugString可以打印血缘关系
    val lines = sc.textFile("datas/word")
    println(lines.toDebugString)
    println("============================")


    val words = lines.flatMap(_.split(" "))
    println(words.toDebugString)
    println("============================")

    val wordToOne = words.map(
      word => (word, 1))
    println(wordToOne.toDebugString)
    println("============================")


    val wordToCount = wordToOne.reduceByKey(_ + _)
    println(wordToCount.toDebugString)
    println("============================")


    val array:Array[(String,Int)]=wordToCount.collect()
    array.foreach(println)
    //TODO 关闭连接
    sc.stop();





  }

}
