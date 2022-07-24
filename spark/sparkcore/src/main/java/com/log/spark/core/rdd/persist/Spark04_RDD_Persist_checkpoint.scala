package com.log.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist_checkpoint {
  def main(args: Array[String]): Unit = {

    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 持久化操作        检查点checkpoint
    //检查点数据需要落盘,路径保存的文件在作业执行完毕后，不会被删除
      sc.setCheckpointDir("cp")//指定路径,一般是在分布式存储系统中如HDFS

    val list = List("Hello Scala", "Hello Spark")

    val rdd: RDD[String] = sc.makeRDD(list,2)
    val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = flatRDD.map(
      word => {
        println("执行一次map操作")
        (word, 1)
      }
    )
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("=====================================")



    sc.stop()

  }


}
