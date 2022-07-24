package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -sortBy
    //加一个第二个参数false可以倒序排,字符串可以toInt根据数字大小排序
    //默认分区数量不变，但是有shuffle
    val rdd= sc.makeRDD(List(("1",1),("11",12),("2",3)), 2)
    val newRDD: RDD[(String, Int)] = rdd.sortBy(t => t._1.toInt,false)//降序

    newRDD.collect().foreach(println)
sc.stop()

  }
}
