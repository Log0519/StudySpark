package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -coalesce 缩小分区
    //根据数据量缩减分区，用于大数据过滤后，提高小数据的执行效率
    //但是默认不会重新打乱再分区，这个时候可能仍会导致数据倾斜，可以进行shuffle处理,在后面加true就可以
    //shuffle后分区内会失去规律
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4,5,6), 3)

    val newRDD: RDD[Int] = rdd.coalesce(2,true)
    newRDD.saveAsTextFile("output")



sc.stop()

  }
}
