package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark9_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -distinct
   //去重，不是数据结构，只是分布式处理
   //原理：map(x=>(x,null)).reduceByKey((x,_)=>x,numPartitions).map(_._1)
   val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 2, 3, 3, 4, 5))
    rdd.distinct().collect().foreach(println)


sc.stop()

  }
}
