package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_RDD_Operator_Transform_Key_Value {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    //TODO 算子 reduceByKey KEY VALUE类型
    //性能角度
    //分组聚合场景下reduceByKey比groupByKey更优，都具有shuffle过程
    // 因为支持分区内combine预聚合功能，可以有效减少shuffle时落盘的数据量
    //功能角度
    //如果只需要分组，不聚合，那么久用groupByKey
    //相同的key分在同一个组，对value做聚合
    //scala和spark都是两两聚合,先执行前面两个相加，再把结果和后面一个相加
    //如果key只有一个，那么是不会参与运算的

    val rdd = sc.makeRDD(List(
      ("a",1),("a",2),("a",3),("b",4),
    ))
    val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey((x: Int, y: Int) => {
      println("x="+x)
      println("x="+y)
      x + y
    })
    reduceRDD.collect().foreach(println)






sc.stop()

  }
}
