package com.log.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_IO_Read {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("IO")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1),
      ("b", 2),
      ("c", 3)
    ),2)
    rdd.saveAsTextFile("output/IO/io1")
    rdd.saveAsObjectFile("output/IO/io2")
    rdd.saveAsSequenceFile("output/IO/io3")//必须是键值类型





    sc.stop()
  }

}
