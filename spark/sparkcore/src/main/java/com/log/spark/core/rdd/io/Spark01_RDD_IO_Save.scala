package com.log.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Save {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("IO")
    val sc = new SparkContext(sparkConf)

    val rdd1: RDD[String] = sc.textFile("output/IO/io1")
    println(rdd1.collect().mkString(","))

    val rdd2: RDD[(String, Int)] = sc.objectFile[(String, Int)]("output/IO/io2")
    println(rdd2.collect().mkString(","))

    val rdd3: RDD[(String, Int)] = sc.sequenceFile[String, Int]("output/IO/io3")
    println(rdd3.collect().mkString(","))





    sc.stop()
  }

}
