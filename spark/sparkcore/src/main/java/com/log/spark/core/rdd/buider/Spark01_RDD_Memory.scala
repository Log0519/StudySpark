package com.log.spark.core.rdd.buider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc=new SparkContext(sparkConf)

    //TODO 创建RDD
    //从内存中创建RDD，将内存中集合的数据作为处理的数据源
    //parallelize并行
    val seq= Seq[Int](1,2,3,4)
   //  val rdd: RDD[Int] = sc.parallelize(seq)等价于下面的一行
   val rdd: RDD[Int] = sc.makeRDD(seq)

    rdd.collect().foreach(println)



    //TODO 关闭环境
  sc.stop()
  }

}
