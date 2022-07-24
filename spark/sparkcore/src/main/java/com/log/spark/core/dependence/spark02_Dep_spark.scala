package com.log.spark.core.dependence

import org.apache.spark.{SparkConf, SparkContext}


object spark02_Dep_spark {
  def main(args: Array[String]): Unit = {

    //TODO 建立和spark的链接

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //TODO 执行业务操作
    //toDeBugString可以打印血缘关系
    val lines = sc.textFile("datas/word")
    println(lines.dependencies)
    println("============================")


    val words = lines.flatMap(_.split(" "))
    println(words.dependencies)
    println("============================")

    val wordToOne = words.map(
      word => (word, 1))
    println(wordToOne.dependencies)
    println("============================")


    val wordToCount = wordToOne.reduceByKey(_ + _)
    println(wordToCount.dependencies)
    println("============================")


    val array:Array[(String,Int)]=wordToCount.collect()
    array.foreach(println)
    //TODO 关闭连接
    sc.stop();





  }

}
