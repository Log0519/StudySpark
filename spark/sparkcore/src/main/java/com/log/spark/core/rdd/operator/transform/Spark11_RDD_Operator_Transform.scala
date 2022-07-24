package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -coalesce -repartition 扩大分区
    //第一种
    //coalesce扩大分区，需要shuffle，后面加true
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,5,6), 2)

    //val newRDD: RDD[Int] = rdd.coalesce(3,true)
    //第二种repartition，底层调用coalesce和shuffle
    val newRDD: RDD[Int] = rdd.repartition(3)
    newRDD.saveAsTextFile("output")

sc.stop()

  }
}
