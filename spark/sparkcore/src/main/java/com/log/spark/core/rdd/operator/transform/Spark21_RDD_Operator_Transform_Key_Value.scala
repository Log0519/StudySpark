package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_RDD_Operator_Transform_Key_Value {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -join KEY VALUE类型
    //在类型为（K,V）和（K，W）返回一个相同key对应所有元素连接在一起的(K,(V,W))的RDD
    //如果两个数据源的数据没有匹配上，是其中一个数据源特有的，就不会匹配上
    //如果相同key有多个,出现类似笛卡尔积，每个都会匹配，数据会成几何形增长，导致性能降低
    //TODO 谨慎使用！！！
    //(a,(1,4))
    //(a,(1,7))
    //(b,(2,5))
    //(c,(3,6))
    //与zip不同，拉链zip是根据对应位置
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3)
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4), ("b",5), ("c", 6),("a",7)
    ))
    rdd1.join(rdd2).collect().foreach(println)
    //(a,(1,4))
    //(b,(2,5))
    //(c,(3,6))







sc.stop()

  }
}
