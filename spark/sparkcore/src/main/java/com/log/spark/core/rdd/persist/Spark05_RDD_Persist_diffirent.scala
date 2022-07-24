package com.log.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_RDD_Persist_diffirent {
  def main(args: Array[String]): Unit = {

    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 持久化操作        检查点checkpoint
    //  区别： cache：将数据临时存储在内存中重用，数据不安全
    //        persist：将数据临时存储在磁盘文件中重，涉及到磁盘io，性能较低，数据安全，
    //                 但是临时保存的数据文件在作业执行完毕后会丢失
    //        checkpoint：将数据长久的保存在磁盘文件中进行重用
    //                    涉及到磁盘io，性能较低，数据安全
    //                    为了保证数据安全，所以一般情况下，会独立再执行一遍作业
    //                    为了提高效率，一般情况下，是需要和cache联合使用。


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
    mapRDD.cache()
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)
    println("=====================================")
    val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()
    groupRDD.collect().foreach(println)



    sc.stop()

  }


}
