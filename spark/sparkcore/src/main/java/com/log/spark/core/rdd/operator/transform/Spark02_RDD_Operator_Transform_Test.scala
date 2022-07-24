package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
   //mapPartitions可以求分区内最大值
    
   val mapRDD: RDD[Int] = rdd.mapPartitions(
     iter => {
       List(iter.max).iterator

     }
   )
    mapRDD.collect().foreach(println)





sc.stop()

  }
}
