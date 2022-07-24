package com.log.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val sc = new SparkContext(sparkConf)
    // TODO 累加器 -Acc 分布式共享只写变量
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //val i: Int = rdd.reduce(_ + _)
    //reduce:分区间计算，分区内计算
    //获取默认累加器，SPARK提供的简单的
    val sumAcc: LongAccumulator = sc.longAccumulator("sum")
    val mapRDD: RDD[Int] = rdd.map(
      num => {
        //使用累加器
        sumAcc.add(num)
        num
      }
    )
//获取累加器值
    //少加：转换算子中调用累加器，如果没有行动算子的话，那么不会执行
    //多加：转换算子中调用累加器，如果有多个行动算子的话，那么执行多次
    //一般情况下，累加器会放置在行动算子中进行操作
    mapRDD.collect()
    mapRDD.collect()
    println(sumAcc.value)
    sc.stop()
  }

}
