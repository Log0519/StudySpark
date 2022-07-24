package com.log.spark.core.rdd.buider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_File1 {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc=new SparkContext(sparkConf)

    //TODO 创建RDD
    //从内文件创建RDD，将文件中集合的数据作为处理的数据源
    val rdd: RDD[(String, String)] = sc.wholeTextFiles("datas")
    rdd.collect().foreach(println)




    //TODO 关闭环境
  sc.stop()
  }

}
