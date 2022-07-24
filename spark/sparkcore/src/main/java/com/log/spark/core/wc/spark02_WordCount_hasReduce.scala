package com.log.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}


object spark02_WordCount_hasReduce {
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
    val wordGroup = wordToOne.groupBy(
      t => t._1
    )
    wordGroup.foreach(println)
    val wordToCount=wordGroup.map{
      case  (word,list)=> list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
    }
    //采集结果打印到控制台
wordToCount.foreach(println)

    //TODO 关闭连接
    sc.stop();





  }

}
