package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -sortBy
    //默认分区数量不变，但是有shuffle
    val rdd: RDD[Int] = sc.makeRDD(List(6,2,4,5,3,1), 2)
    rdd.sortBy(num=>num)//按照自身排序

sc.stop()

  }
}
