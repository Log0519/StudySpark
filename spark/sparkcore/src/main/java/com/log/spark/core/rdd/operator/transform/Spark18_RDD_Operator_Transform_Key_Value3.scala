package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_RDD_Operator_Transform_Key_Value3 {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 aggregateByKey KEY VALUE类型 特殊要求！！！
    // TODO aggregateByKey最终的返回数据结果应该和初始值的类型保持一致
    // TODO -算子 mapValues 对key保持不变，对v变，下面用来求平均值
    //TODO 注意aggregate初始值不仅参与分区内计算，还参与分区间计算，
    // 但是aggregateByKey不会参与分区间计算
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), //初始这三个在一个分区
      ("b", 4), ("b", 5), ("a", 6) //这三个在一个分区
    ),2)
//aggregateByKey最终的返回数据结果应该和初始值的类型保持一致
//    val aggRDD: RDD[(String, String)] = rdd.aggregateByKey("")(_ + _, _ + _)
//    aggRDD.collect().foreach(println)
//求相同key数据的平均值=>(a,3),(b,4)
val newRDD: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
  (t, v) => {
    (t._1 + v, t._2 + 1)
  },
  (t1, t2) => {
    (t1._1 + t2._1, t1._2 + t2._2)
  }
)
    //TODO -算子 mapValues 对key保持不变，对v变
    val resultRDD: RDD[(String, Int)] = newRDD.mapValues {
      case (num, cnt) => num / cnt

    }
resultRDD.collect().foreach(println)





sc.stop()

  }
}
