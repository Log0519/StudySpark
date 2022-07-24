package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-map
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
      //长字符串->短
      val mapRDD: RDD[String] = rdd.map(
        line => {
          val datas: Array[String] = line.split(" ")
          datas(6)
         
        }
      )
mapRDD.collect().foreach(println)


sc.stop()


  }
}