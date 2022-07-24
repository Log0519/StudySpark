package com.log.spark.core.rdd.buider

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory_Par {
  def main(args: Array[String]): Unit = {

    //TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    sparkConf.set("spark.default.parallelism","5")//设置分区数
    val sc=new SparkContext(sparkConf)

    //TODO 创建RDD
   //RDD的并行度&分区,makeRDD不传第二个值，表示的分区数量，就有默认值
   //默认值scheduler.conf.getInt("spark.default.parallelism",totalCores),就是一看是准备环境中的sparkConf
   //默认取环境中的配置，如果没配，就取默认totalCores，这个属性取值为当前运行环境的最大可用核数
   val rdd: RDD[Int] = sc.makeRDD(
     List(1, 2, 3, 4), 2
   )
    rdd.saveAsTextFile("output")



    //TODO 关闭环境
  sc.stop()
  }

}
