package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark6_RDD_Operator_Transform1 {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -groupBy
    //分组和分区没有必然的关系，分组时分区不会改变，但是数据会被打乱重组，这个过程也称为 TODO shuffle
    //一个组在一个分区中，但是一个分区不一定只有一个组
    //根据相同首字母分组
    val rdd = sc.makeRDD(List("Hello","Spark","Scala","Hadoop"),2)

    val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(_.charAt(0))
    groupRDD.collect().foreach(println)









sc.stop()

  }
}
