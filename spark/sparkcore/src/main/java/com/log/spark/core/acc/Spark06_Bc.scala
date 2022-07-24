package com.log.spark.core.acc

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark06_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val sc = new SparkContext(sparkConf)
    // TODO 累加器 -Bc 分布式共享只读变量

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val map: mutable.Map[String, Int] = mutable.Map(("a", 4), ("b", 5), ("c", 6))

    // TODO 累加器 -Bc 实现
    //3.Executor其实就是一个JVM，所以在启动的时候，会自动分配内存，
    // 把闭包数据放在Executor中就可以了，用来共享=>广播变量可以
    //广播变量进行不能更改，是只读的

    //封装广播变量
    val bc: Broadcast[mutable.Map[String, Int]] = sc.broadcast(map)

    rdd1.map{
      case (w,c)=>{
        val l: Int = bc.value.getOrElse(w, 0)//访问广播变量
        (w,(c,l))
      }
    }.collect().foreach(println)

    sc.stop()
  }

}
