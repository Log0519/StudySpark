package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Part {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-map
    //验证rdd转换算子分区前后不变
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    rdd.saveAsTextFile("output1")
    val mapRDD: RDD[Int] = rdd.map(_ * 2)
    mapRDD.saveAsTextFile("output2")

sc.stop()


  }
}