package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_RDD_Operator_Transform_Key_Value {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 aggregateByKey KEY VALUE类型 分区内外不同版
    //TODO 注意aggregate初始值不仅参与分区内计算，还参与分区间计算，
    // 但是aggregateByKey不会参与分区间计算
    //过程为分区内先计算(预计算)，然后经过shuffle打乱重组，进行分区间计算
    // reduceByKey需要分区内和分区内操作相同，如果想要现在分区内求最大值，再在分区间求最大值的和，就不行
    //可以选择使用aggregateByKey
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), //初始这三个在一个分区
      ("b", 4), ("b", 5), ("a", 6) //这三个在一个分区
    ),2)
    //aggregateByKey存在函数的柯里化，有两个参数列表
    //第一个参数列表,需要传递一个参数，表示为初始值
    //scala和spark都是两两聚合,先执行前面两个相加，再把结果和后面一个相加
    //初始值用来当碰见第一个key的时候，和value进行分区内计算
    //第二个参数列表需要传递两个参数，第一个参数表示分区内计算规则，第二个表示分区间计算规则

    rdd.aggregateByKey((0))(
      (x,y)=>{math.max(x,y)},
      (x,y)=>{x+y}
    ).collect().foreach(println)








sc.stop()

  }
}
