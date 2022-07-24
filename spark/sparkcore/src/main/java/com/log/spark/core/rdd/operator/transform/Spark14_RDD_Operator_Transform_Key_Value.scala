package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_RDD_Operator_Transform_Key_Value {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 partitionBy KEY VALUE类型
    //根据指定规则对数据重分区

    val rdd = sc.makeRDD(List(1, 2, 3, 4),2)
    val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

    //partitionBY不是RDD类型的，存在隐式转换（二次编译）
    //RDD=>
    mapRDD.partitionBy(new HashPartitioner((2))).saveAsTextFile("output")










sc.stop()

  }
}
