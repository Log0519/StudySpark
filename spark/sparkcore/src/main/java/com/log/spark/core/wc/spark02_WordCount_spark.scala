package com.log.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}


object spark02_WordCount_spark {
  def main(args: Array[String]): Unit = {

    //TODO 建立和spark的链接

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)

    //TODO 执行业务操作
    //1.按行读取文件
    //hello word
    val lines = sc.textFile("datas")
    //2.分词
    //hello,word,hello,word
    //扁平化
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map(
      word => (word, 1)
    )

    //spark框架提供了更多的功能，可以将分组和聚合使用一个方法实现
    //reduceByKey相同key的数据可以对value进行reduce聚合
    val wordToCount = wordToOne.reduceByKey(_ + _)

    //采集结果打印到控制台
    val array:Array[(String,Int)]=wordToCount.collect()
    array.foreach(println)
    //TODO 关闭连接
    sc.stop();





  }

}
