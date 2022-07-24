package com.log.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action_Some {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 行动算子 -1.reduce, 2.collect,3.count,4.first,5.take,
    // 6.takeOrdered
    //会直接出结果,不是生成一个新的rdd
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    //1.reduce
    //聚合
    val i: Int = rdd.reduce(_ + _)
    println("reduce: "+i)

    //2.collect
    //将不同分区的数据，按照分区数据顺序，采集到Driver端内存中，形成数组
    val ints: Array[Int] = rdd.collect()
    println("collect: "+ints.mkString(","))

    //3.count
    //返回RDD中元素的个数
    val l: Long = rdd.count()
    println("count: "+l)

    //4.first
    //返回第一个元素
    val i1: Int = rdd.first()
    println("first: "+i1)

    //5.take
    //返回前n个元素
    val ints1: Array[Int] = rdd.take(3)
    println("take: "+ints1.mkString(","))

    //6.takeOrdered
    //返回该RDD排序后的前n个元素组成的数组
    //默认是升序，如果要降序,加上第二个参数(Ordering.Int.reverse)
    val rdd1: RDD[Int] = sc.makeRDD(List(3, 2, 4, 5))
    val ints2: Array[Int] = rdd1.takeOrdered(3)
    println("takeOrdered: "+ints2.mkString(","))





    sc.stop()

  }
}
