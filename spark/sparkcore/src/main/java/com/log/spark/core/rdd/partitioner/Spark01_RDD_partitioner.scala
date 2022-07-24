package com.log.spark.core.rdd.partitioner

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark01_RDD_partitioner {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Partitioner")
    val sc = new SparkContext(sparkConf)
    val rdd: RDD[(String, String)] = sc.makeRDD(List(
      ("nba", "xxxxxxxx1"),
      ("cba", "xxxxxxxx"),
      ("wnba", "xxxxxxxx"),
      ("nba", "xxxxxxxx2")
    ),3)
    val partRDD: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner)
    partRDD.saveAsTextFile("output/MyPartitioner")





    sc.stop()
  }
  /*自定义分区器
  1.继承Partitioner
  2.重写方法
  * */

  class MyPartitioner extends org.apache.spark.Partitioner{
    //分区数量
    override def numPartitions: Int = 3
    //根据数据的key值返回数据所在的分区索引（从0开始）
    override def getPartition(key: Any): Int = {
      key match {
        case "nba"=>0
        case "wnba"=>1
        case _=>2
      }
//      if(key=="nba"){
//        0
//      }else if(key=="wnba"){
//        1
//      }else if(key=="cba"){
//        2
//      }else{
//        2
//      }
    }
  }
}
