package com.log.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc_noAcc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val sc = new SparkContext(sparkConf)
// TODO 累加器 -Acc 分布式共享只写变量
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //val i: Int = rdd.reduce(_ + _)
    //reduce:分区间计算，分区内计算
    var sum=0
    rdd.foreach(
      num=>{
        sum+=num
      }
    )

    println("sum="+sum)

    sc.stop()
  }

}
