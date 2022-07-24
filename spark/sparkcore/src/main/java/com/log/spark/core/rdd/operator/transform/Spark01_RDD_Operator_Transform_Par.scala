package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Par {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-map
    val rdd=sc.makeRDD(List(1,2,3,4))

    val mapRDD= rdd.map(
      num => {
        println("<<<<<" + num)
        num
      }
    )
    val mapRDD2= mapRDD.map(
      num => {
        println("=====" + num)
        num
      }
    )
    mapRDD.collect()
    mapRDD2.collect()
         




sc.stop()


  }
}