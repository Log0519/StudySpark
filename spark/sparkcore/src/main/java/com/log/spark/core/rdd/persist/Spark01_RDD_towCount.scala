package com.log.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_towCount {
  def main(args: Array[String]): Unit = {

    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
//TODO 持久化操作        无持久化重复使用RDD版
//TODO 持久化操作是在行动算子执行的时候完成的，在数据执行较长或者重要的场合，也可以用持久化操作
    val list = List("Hello Scala", "Hello Spark")

    val rdd: RDD[String] = sc.makeRDD(list)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map((_, 1))
    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("=====================================")
    val rdd1: RDD[String] = sc.makeRDD(list)
    val flatRDD1: RDD[String] = rdd1.flatMap(_.split(" "))
    val mapRDD1: RDD[(String, Int)] = flatRDD1.map((_, 1))
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD1.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()

  }


}
