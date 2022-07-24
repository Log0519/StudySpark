package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark7_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -filter
   //获取日志中5月17日的请求路径
   val rdd: RDD[String] = sc.textFile("datas/apache.log")
    rdd.filter(
      line=>{
        //获取时间数据
        val datas: Array[String] = line.split(" ")
        val time: String = datas(3)
        time.startsWith("17/05/2015")
      }
    ).collect().foreach(println)


sc.stop()

  }
}
