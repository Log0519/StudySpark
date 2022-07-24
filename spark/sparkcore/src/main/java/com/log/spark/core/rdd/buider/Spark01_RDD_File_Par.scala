package com.log.spark.core.rdd.buider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_File_Par {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc=new SparkContext(sparkConf)

    //TODO 创建RDD
    //spark底层读取文件采用的是hadoop的方式
    //分区数量计算方式：如果设定为2，是最小分区值为2，不一定结果是2
    val rdd: RDD[String] = sc.textFile("datas/1.txt")



    //TODO 关闭环境
  sc.stop()
  }

}
