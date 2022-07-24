package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform_Key_Value2 {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO 算子 foldByKey KEY VALUE类型 aggregateByKey分区内外的操作相同的简化版
    //TODO 注意aggregate初始值不仅参与分区内计算，还参与分区间计算，
    // 但是aggregateByKey不会参与分区间计算
    //如果分区内和分区间计算规则相同，spark提供了简化的操作，foldByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), //初始这三个在一个分区
      ("b", 4), ("b", 5), ("a", 6) //这三个在一个分区
    ),2)
    //rdd.aggregateByKey((0))(_+_, _+_).collect().foreach(println)
rdd.foldByKey(0)(_+_).collect().foreach(println)







sc.stop()

  }
}
