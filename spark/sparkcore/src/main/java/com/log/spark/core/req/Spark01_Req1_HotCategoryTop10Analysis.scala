package com.log.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Req1_HotCategoryTop10Analysis {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)


    //TODO 需求 1：Top10 热门品类
    //1.读取原始的日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    //2019-07-17 _ 95 _ 26070e87-1ad7-49a3-8fb3-cc741facaddf _ 37 _ 2019-07-17 00:00:02 _ 手机 _ -1 _ -1 _ null _ null _ null _ null _ 3
    //2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_48_2019-07-17 00:00:10_null_16_98_null_null_null_null_19
    //2019-07-17_95_26070e87-1ad7-49a3-8fb3-cc741facaddf_6_2019-07-17 00:00:17_null_19_85_null_null_null_null_7

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
    val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = {
      clickCountRDD.cogroup(orderCountRDD, payCountRDD)
    }

      val analysisRDD: RDD[(String, (Int, Int, Int))] = cogroupRDD.mapValues {
        case (clickIter, orderIter, payIter) => {
          //1.
          var clickCnt = 0
          val iter1 = clickIter.iterator
          if (iter1.hasNext) {
            clickCnt = iter1.next()
          }

          //2.
          var orderCnt = 0
          val iter2 = orderIter.iterator
          if (iter2.hasNext) {
            orderCnt = iter2.next()
          }
          //3.
          var payCnt = 0
          val iter3 = payIter.iterator
          if (iter3.hasNext) {
            payCnt = iter3.next()
          }

          (clickCnt, orderCnt, payCnt)

        }
      }
      //false降序
      //最终结果
      val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
    //6.将结果采集到控制台打印出来
      resultRDD.foreach(println)







    sc.stop()

  }
}
