package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //获取第二个分区的数据
    //TODO 算子-mapPartitionsWithIndex
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)
    val mapRDD: RDD[Int] = rdd.mapPartitionsWithIndex(
      (index , iter)=>{
        if(index==1){
          iter
        }else{
          Nil.iterator
        }
      }
    )
mapRDD.collect().foreach(println)

sc.stop()

  }
}
