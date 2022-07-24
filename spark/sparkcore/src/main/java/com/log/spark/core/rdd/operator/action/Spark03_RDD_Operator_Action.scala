package com.log.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 行动算子 -1.aggregate,2.fold
    //TODO 注意aggregate初始值不仅参与分区内计算，还参与分区间计算，
    // 但是aggregateByKey不会参与分区间计算
    //1.aggregate
    //初始值
    //分区内方法
    //分区间方法
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val i: Int = rdd.aggregate(1)(_ + _, _ + _)
println(i)

    //2.fold
    //aggregate的简化版，用于分区间和分区内操作相同时
    val i1: Int = rdd.fold(0)(_ + _)
println(i1)




    sc.stop()

  }
}
