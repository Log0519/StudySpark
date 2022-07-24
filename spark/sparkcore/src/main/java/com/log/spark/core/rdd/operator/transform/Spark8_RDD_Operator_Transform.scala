package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark8_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -sample
    //可以用于数据倾斜的场景
    //根据规则从数据集中抽取数据,随机
    //第一个参数：抽取后是否放回 true or false
    //第二个:分值比率，如果是抽取不放回，表示每条数据抽取的概率。如果放回，表示数据源中
    // 的每条数据被抽取的可能次数,可能多次，或者0次
    //第三个：随机数种子,不传是根据当前系统时间随机的，传了后是固定的
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
//    println(rdd.sample(
//      false,
//      0.4,
//      //1
//    ).collect().mkString(","))
    println(rdd.sample(
      true,
      2,
      //1
    ).collect().mkString(","))


sc.stop()

  }
}
