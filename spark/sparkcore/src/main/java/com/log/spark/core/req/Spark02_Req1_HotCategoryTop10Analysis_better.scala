package com.log.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Req1_HotCategoryTop10Analysis_better {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)


    //TODO 需求 1：Top10 热门品类  优化版本
    //TODO 问题：1.actionRDD重复使用，该版本解决
    //          2.cogroup存在shuffle，性能可能较低，该版本解决
    //1.读取原始的日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    //TODO 优化问题 1
    actionRDD.cache()

    //2.统计品类的点击数量：(品类ID，点击数量)
    //去除非点击
    val clickActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(6) != "-1"
      }
    )
    //品类ID和点击数量
    val clickCountRDD: RDD[(String, Int)] = clickActionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        (datas(6), 1)
      }
    ).reduceByKey(_+_)

    //3.统计品类的下单数量：(品类ID，下单数量)
    //去除非下单
    val orderActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(8) != "null"
      }
    )
    //下单多个品类会用逗号隔开
//(32,12,34)=>(32,1),(12,1),(34,1)
    val orderCountRDD: RDD[(String, Int)] = orderActionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val cid: String = datas(8)
        val cids: Array[String] = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    //4.统计品类的支付数量：(品类ID，支付数量)
    val payActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        datas(10) != "null"
      }
    )
    val payCountRDD: RDD[(String, Int)] = payActionRDD.flatMap(
      action => {
        val datas: Array[String] = action.split("_")
        val cid: String = datas(10)
        val cids: Array[String] = cid.split(",")
        cids.map(id => (id, 1))
      }
    ).reduceByKey(_ + _)

    //5.将品类进行排序，并且取前10名
    //点击数量排序，下单数量排序，支付数量排序
    //元组排序：先比较第一个，相同再比较第二个.......
      //(品类ID,(点击数量，下单数量，支付数量))
      //join,zip,leftOuterJoin,cogroup
    //join不能用，可能有点击，但不一定有下单，下单了不一定有支付
    //zip，不行
    //leftOuterJoin不确定左右，不能使用
    //cogroup可以，在自己的数据源中建立一个分组和另外一个数据源连接，=connect+group，即使数据不存在，也有分组在里面
    //TODO 优化问题 2
    //（品类，点击数量），（品类，下单数量），（品类，支付数量）=>
    //（品类,(点击数量,0,0)),(品类,(0,下单数量,0)),(品类，（0,0,支付数量))
    val rdd1: RDD[(String, (Int, Int, Int))] = clickCountRDD.map {
      case (cid, cnt) => (cid, (cnt, 0, 0))
    }
    val rdd2: RDD[(String, (Int, Int, Int))] = orderCountRDD.map {
      case (cid, cnt) => (cid, (0,cnt, 0))
    }
    val rdd3: RDD[(String, (Int, Int, Int))] = payCountRDD.map {
      case (cid, cnt) => (cid, (0,0,cnt))
    }
    //将三个数据源合并在一起,统一进行聚合计算
    val sourceRDD: RDD[(String, (Int, Int, Int))] = rdd1.union(rdd2).union(rdd3)
    val analysisRDD: RDD[(String, (Int, Int, Int))] = sourceRDD.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
      //false降序
      //最终结果
      val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
    //6.将结果采集到控制台打印出来
      resultRDD.foreach(println)







    sc.stop()

  }
}
