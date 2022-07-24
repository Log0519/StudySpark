package com.log.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Req1_HotCategoryTop10Analysis_better2 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)


    //TODO 需求 1：Top10 热门品类  优化版本
    //TODO 问题：1.actionRDD重复使用,已解决
    //          2.cogroup存在shuffle，性能可能较低，已解决
    //          3.大量的reduceBykey，大量的shuffle，如果是相同的数据源，spark提供了优化、缓存，
    //          但是这里是不相同的数据源，该版本解决
    //1.读取原始的日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    //TODO 优化问题 3 只有一个reduceByKey
    //2.将数据转换结构
    //点击的场合：（品类id，（1,0,0））
    //下单的场合：（品类id，（0,1,0））
    //支付的场合：（品类id，（0,0,1））
    val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          //点击的场合
          List((datas(6), (1, 0, 0)))
        }
        else if (datas(8) != "null") {
          //下单的场合
          val ids: Array[String] = datas(8).split(",")
          ids.map(id => (id, (0, 1, 0)))
        } else if (datas(10) != "null") {
          //支付的场合
          val ids: Array[String] = datas(10).split(",")
          ids.map(id => (id, (0, 0, 1)))
        } else {
          Nil
        }
      }
    )

    //3.将相同品类id的数据进行分组聚合
    //(品类ID，（点击数量，下单数量，支付数量）)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    //4.将统计结果进行降序排列，取前十个
    val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
    //5.采集打印
    resultRDD.foreach(println)
    sc.stop()

  }
}
