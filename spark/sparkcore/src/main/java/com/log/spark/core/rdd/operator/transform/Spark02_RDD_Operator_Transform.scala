package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 算子-mapPartitions
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    //rdd.map是一个一个对数据操作，类似于串行，mapPartitions是对一个分区内的数据操作
   //类似缓冲区，提高效率,可以让每个分区只执行一次流程，性能更高，但是占内存，会把整个分区数据加载到内存进行引用
  //处理完是不会释放的，存在对象的引用
    //可能导致内存溢出

   val mapRDD: RDD[Int] = rdd.mapPartitions(
     iter => {
       println("<<<<<<<<<<<<<<")
       iter.map(_ * 2)
     }
   )
    mapRDD.collect().foreach(println)





sc.stop()

  }
}
