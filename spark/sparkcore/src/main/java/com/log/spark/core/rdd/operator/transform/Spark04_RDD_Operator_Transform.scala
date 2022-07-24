package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子-flatMap
    //把数据3处理成list类型，再进行扁平化操作
    val rdd = sc.makeRDD(List(List(1, 2),3,List(4,5)))
    val flatRDD: RDD[Any] = rdd.flatMap(
      data => {
        data match {
          case list: List[_] => list
          case dat => List(dat)
        }
      }
    )

flatRDD.collect().foreach(println)




sc.stop()

  }
}
