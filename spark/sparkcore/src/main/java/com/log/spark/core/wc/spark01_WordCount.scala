package com.log.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object spark01_WordCount {
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

    //3.将数据根据单词进行分组便于统计
    //(hello,hello,hello),(word,word)
    val wordGroup = words.groupBy(word => word)

    //4.分组后进行转换
    //(hello,hello,hello),(word,word)=>(hello,3),(word,2)s
    val wordToCount = wordGroup.map {
      case (word, list) => (word, list.size)
    }

    //5.采集结果打印到控制台
    val array = wordToCount.collect()
    array.foreach(println)

    //TODO 关闭连接
    sc.stop();





  }

}
