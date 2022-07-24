package com.log.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Persist_isPersist {
  def main(args: Array[String]): Unit = {

    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO 持久化操作        持久化重复使用RDD版
    //TODO 持久化操作是在行动算子执行的时候完成的，在数据执行较长或者重要的场合，也可以用持久化操作
    val list = List("Hello Scala", "Hello Spark")

    val rdd: RDD[String] = sc.makeRDD(list)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map(
      word => {
        println("执行一次map操作")
        (word, 1)
      }
    )
    //放在内存中
    //cache默认持久化保存在内存中,如果想要设置存储级别
    //就用persist，参数加上StorageLevel.

    //TODO 持久化操作是在行动算子执行的时候完成的，在数据执行较长或者重要的场合，也可以用持久化操作
    mapRDD.cache()
    mapRDD.persist(StorageLevel.DISK_ONLY)

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("=====================================")

    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)

    sc.stop()

  }


}
