package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_RDD_Operator_Transform_Key_Value {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 四种 KEY VALUE类型对比
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("a", 2), ("b", 3), //初始这三个在一个分区
      ("b", 4), ("b", 5), ("a", 6) //这三个在一个分区
    ),2)
    rdd.reduceByKey(_+_)//wordcount，分区间，分区内计算规则相同
    rdd.aggregateByKey(0)(_+_,_+_)//wordcount，
    rdd.foldByKey(0)(_+_)//wordcount，分区间、内用同一个规则计算，2个参数
    rdd.combineByKey(v=>v,(x:Int,y:Int)=>x+y,(x:Int,y:Int)=>x+y)//wordcount
    //四个方法底层调用方法完全一样,
    //1.相同key第一条value的处理函数
    //2.分区内
    //2.分区间







sc.stop()

  }
}
