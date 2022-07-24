package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -glom
    val rdd = sc.makeRDD(List(1,2,3,4),2)
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    //两个分区的最大值相加
    val maxRDD: RDD[Int] = glomRDD.map(
      array => {
        array.max
      }
    )

    println(maxRDD.collect().sum)








sc.stop()

  }
}
