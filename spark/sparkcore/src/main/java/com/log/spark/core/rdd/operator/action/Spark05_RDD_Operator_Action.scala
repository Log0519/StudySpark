package com.log.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 行动算子 -save相关
   // val rdd: RDD[(String,Int)] = sc.makeRDD(List(
      //("a", 1), ("a", 2), ("a", 3), 2))
//    rdd.saveAsTextFile("output")
//    rdd.saveAsObjectFile("output2")
//    rdd.saveAsSequenceFile("output3")//K-V类型



    sc.stop()

  }
}
