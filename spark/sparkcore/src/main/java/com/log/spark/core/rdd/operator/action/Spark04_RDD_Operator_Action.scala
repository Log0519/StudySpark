package com.log.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 行动算子 -1.countByValue,2.countByKey
    //1.countByValue
    //计算每个值出现的次数
    //Map(4 -> 1, 2 -> 1, 1 -> 1, 3 -> 1)
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val map: collection.Map[Int, Long] = rdd.countByValue()
    println(map)

    //2.countByKey
    //计算每个Key出现的次数
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 1), ("a", 1)
    ), 2)
    val map1: collection.Map[String, Long] = rdd1.countByKey()
println(map1)


    sc.stop()

  }
}
