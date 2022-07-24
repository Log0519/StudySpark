package com.log.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 行动算子 -foreach相关
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))
    rdd.collect().foreach(println)//collect以分区顺序采集,Driver端
    println("=============================")
    rdd.foreach(println)//无顺序,Executor端,可以分布式打印，发送给不同的Executor

//算子：Operator
    //原因是RDD的方法和Scala集合对象的方法不一样
    //集合对象的方法都是在同一个节点的内存中完成的
    //RDD的方法可以将计算逻辑发送到Executor端（分布式）执行
    //叫算子是为了区分不同的处理效果，RDD称为算子
    //RDD的方法外部的操作都是在Driver端执行
      //RDD方法内部的逻辑代码是在Executor端执行



    sc.stop()

  }
}
