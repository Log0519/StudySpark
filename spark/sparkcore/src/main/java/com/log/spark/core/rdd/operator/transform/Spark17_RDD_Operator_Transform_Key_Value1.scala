package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform_Key_Value1 {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 aggregateByKey KEY VALUE类型 分区内外相同版
    //TODO 注意aggregate初始值不仅参与分区内计算，还参与分区间计算，
    // 但是aggregateByKey不会参与分区间计算
    //aggregateByKey也可以分区内和分区间进行相同操作
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), //初始这三个在一个分区
      ("b", 4), ("b", 5), ("a", 6) //这三个在一个分区
    ),2)

    rdd.aggregateByKey((0))(_+_, _+_).collect().foreach(println)








sc.stop()

  }
}
