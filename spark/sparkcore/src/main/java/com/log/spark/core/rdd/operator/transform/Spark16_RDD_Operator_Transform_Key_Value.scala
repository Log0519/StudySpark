package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_RDD_Operator_Transform_Key_Value {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 groupByKey KEY VALUE类型
    //相同的key分在同一个组,形成一个对偶元组,第一个元素是key,第二个元素是相同key的value的集合
    //reduceByKey需要分区内和分区内操作相同，如果想要现在分区内求最大值，再在分区间求最大值的和，就不行

    val rdd = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("a", 3), ("b", 4),
    ))
    val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
    val groopRDD2: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)


    groupRDD.collect().foreach(println)
    groopRDD2.collect().foreach(println)











sc.stop()

  }
}
