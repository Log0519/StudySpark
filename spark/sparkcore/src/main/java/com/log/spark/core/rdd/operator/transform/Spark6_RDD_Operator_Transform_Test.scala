package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object Spark6_RDD_Operator_Transform_Test {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)
    // TODO 算子 -groupBy
    //从日志中提取每个时间段的访问量
    val rdd: RDD[String] = sc.textFile("datas/apache.log")
    val timeRDD: RDD[(String, Iterable[(String, Int)])] = rdd.map(
      line => {
        val datas: Array[String] = line.split(" ")
        val time: String = datas(3)
        //time.substring(0,)
        val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val date: Date = sdf.parse(time)
        val sdf1 = new SimpleDateFormat("HH")
        val hour: String = sdf1.format(date)
        (hour, 1)
      }
    ). //reduceByKey(_+_)
      groupBy(_._1)

    timeRDD.map{
      case (hour,iter)=>{
        (hour,iter.size)
      }
    }.collect().sortBy(_._1).foreach(println)



sc.stop()

  }
}
