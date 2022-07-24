package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 -glom
    //把分区数组转换到相同类型的内存数组进行处理，分区不变
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    //打印方式:
    glomRDD.collect().foreach(data=>println(data.mkString(",")))








sc.stop()

  }
}
