package com.log.spark.core.rdd.buider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_File {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc=new SparkContext(sparkConf)

    //TODO 创建RDD
    //从内文件创建RDD，将文件中集合的数据作为处理的数据源
    //path路径默认以当前环境的根路径为基准。可以写绝对路径，也可以写相对路径
    //path可以是文件的具体路径，也可以是目录的名称
    //val rdd: RDD[String] = sc.textFile("datas/1.txt")
    //val rdd: RDD[String] = sc.textFile("datas")
    //path路径还可以使用通配符
    //val rdd: RDD[String] = sc.textFile("datas/1*.txt")
    val rdd: RDD[String] = sc.textFile("hdfs://hadoop102:8020/test.txt")
    rdd.collect().foreach(println)
    //还可以是分布式存储路径




    //TODO 关闭环境
  sc.stop()
  }

}
