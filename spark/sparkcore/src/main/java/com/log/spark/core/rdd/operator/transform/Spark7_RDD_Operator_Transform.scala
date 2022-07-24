package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object Spark7_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -filter
    //筛选过滤操作,符合规则的保留，分区不变，但是分区内的数据可能不均衡，可能出现数据倾斜
    //留下奇数，偶数不要
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val filterRDD: RDD[Int] = rdd.filter(num => num % 2 != 0)
    filterRDD.collect().foreach(println)


sc.stop()

  }
}
