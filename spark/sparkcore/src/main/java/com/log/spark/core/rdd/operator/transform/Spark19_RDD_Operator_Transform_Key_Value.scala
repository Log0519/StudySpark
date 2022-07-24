package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_RDD_Operator_Transform_Key_Value {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 combineByKey KEY VALUE类型 不需要初始值，可以转换第一个元素
    //取消aggregateByKey的初始值，改用一种转换
    //有三个参数
    // 第一个参数：将相同key的第一个数据进行结构转换，实现操作（替代初始值）
    // 第二个参数：分区内的计算规则
    // 第三个参数：分区间的计算规则
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), //初始这三个在一个分区
      ("b", 4), ("b", 5), ("a", 6) //这三个在一个分区
    ),2)

val newRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
  v=>(v,1),//动态识别时要提前定义好参数类型，否则可能出现错误
  (t:(Int,Int), v) => {
    (t._1 + v, t._2 + 1)
  },
  (t1:(Int,Int), t2:(Int,Int)) => {
    (t1._1 + t2._1, t1._2 + t2._2)
  }
)
    //对key保持不变，对v变
    val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
      case (num, cnt) => num / cnt
    }
resultRDD.collect().foreach(println)





sc.stop()

  }
}
