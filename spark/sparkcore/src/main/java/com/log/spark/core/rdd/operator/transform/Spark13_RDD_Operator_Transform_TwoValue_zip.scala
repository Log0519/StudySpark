package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_RDD_Operator_Transform_TwoValue_zip {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -双 value 类型

    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,5,6),2)
    val rdd2: RDD[Int] = sc.makeRDD(List(3, 4, 5, 6),2)
    //Can't zip RDDs with unequal numbers of partitions: List(2, 4)
    //拉链zip的两个数据源要求分区数量要相等
    //Can only zip RDDs with same number of elements in each partition
    //在spark中，两个数据源的分区中的数据量要保持一致，scala可以不用
    val rdd7: RDD[String] = sc.makeRDD(List("3", "4", "5", "6"))

    //拉链,把两个集合的数据分别放一起
    val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
    println(rdd6.collect().mkString(","))


sc.stop()

  }
}
