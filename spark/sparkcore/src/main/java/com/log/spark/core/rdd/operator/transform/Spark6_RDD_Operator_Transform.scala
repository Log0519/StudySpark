package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark6_RDD_Operator_Transform {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -groupBy
    //根据返回的分组key进行分组，相同的key的数会放在一组
    //实现奇数和偶数的分组
    val rdd = sc.makeRDD(List(1,2,2,3,4,3,2,2,3,4),2)
    def groupFunction(num:Int)={
        num%2
    }

    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)
    groupRDD.collect().foreach(println)









sc.stop()

  }
}
