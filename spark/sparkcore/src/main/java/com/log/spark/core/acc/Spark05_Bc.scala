package com.log.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark05_Bc {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val sc = new SparkContext(sparkConf)
// TODO 累加器 -Bc 分布式共享只读变量  实现在06

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
//    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
//      ("a", 4), ("b", 5), ("c", 6)
//    ))

    //1.join会导致数据量几何增长，并且会影响shuffle性能，不推荐使用
//    val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
//    joinRDD.collect().foreach(println)
  val map=mutable.Map(("a", 4), ("b", 5), ("c", 6))

    //2.特殊处理，无shuffle，小数据性能很好
    //但是每个任务中包含闭包数据，如果每一个Task都用到了map，会各存一份map，
    // 如果多个Task运行在一个Executor上，Executor就有很多冗余的数据，并且占用大量内存
    //引入bc
    rdd1.map{
      case (w,c)=>{
        val l: Int = map.getOrElse(w, 0)
        (w,(c,l))
      }
    }.collect().foreach(println)

    // TODO 累加器 -Bc 分布式共享只读变量
    //3.Executor其实就是一个JVM，所以在启动的时候，会自动分配内存，
    // 把闭包数据放在Executor中就可以了，用来共享=>广播变量可以
    //广播变量进行不能更改，是只读的


    sc.stop()
  }

}
