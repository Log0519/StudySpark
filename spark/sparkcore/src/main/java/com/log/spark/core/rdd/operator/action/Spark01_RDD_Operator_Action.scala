package com.log.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 行动算子 -
    //行动算子其实是触发作业执行的方法
    //底层调用的是环境对象的runJob方法
    //底层代码会创建ActiveJob，并提交执行
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    rdd.collect()




    sc.stop()


  }
}
