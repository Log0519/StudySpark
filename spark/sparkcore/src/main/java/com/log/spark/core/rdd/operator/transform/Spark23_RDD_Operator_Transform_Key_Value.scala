package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_RDD_Operator_Transform_Key_Value {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -cogroup KEY VALUE类型\
    //cogroup：connect+group
    //相同的key放在一个组中，连接在一起

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4), ("b",5)//, ("c", 6)
    ))
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
cogroupRDD.collect().foreach(println)
    //(a,(CompactBuffer(1),CompactBuffer(4)))
    //(b,(CompactBuffer(2),CompactBuffer(5)))
    //(c,(CompactBuffer(3),CompactBuffer()))


sc.stop()

  }
}
