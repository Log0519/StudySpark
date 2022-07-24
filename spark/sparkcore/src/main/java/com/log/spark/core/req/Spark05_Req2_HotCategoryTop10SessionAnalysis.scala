package com.log.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Req2_HotCategoryTop10SessionAnalysis {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)


    //TODO 需求 2：Top10 热门品类中每个品类的 Top10 活跃 Session 统计
    //1.读取原始的日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")

    actionRDD.cache()
    val top10Ids: Array[String] = top10Category(actionRDD)

    //1.过滤原始数据,保留点击和前10的品类ID
    val filterActionRDD: RDD[String] = actionRDD.filter(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          top10Ids.contains(datas(6))
        } else {
          false
        }
      }
    )
    //2.根据品类ID和sessionid进行点击量的统计，一个用户在某个地方点击了一次
    val reduceRDD: RDD[((String, String), Int)] = filterActionRDD.map(
      action => {
        val datas: Array[String] = action.split("_")
        ((datas(6), datas(2)), 1)
      }
    ).reduceByKey(_ + _)

    //3.将统计的结果进行结构的转换
    //（（品类ID，sessionId），sum）=>(品类ID,(sessionId,sum))
    val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((cid, sid), sum) => (cid, (sid, sum))
    }

    //4.相同的品类放在一起
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()

    //5.将分组后的数据进行点击量排序，取前十名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
      }
    )

    resultRDD.collect().foreach(println)

    sc.stop()

  }
  def top10Category(actionRDD:RDD[String])={

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
    //4.将统计结果进行降序排列，取前十个,直接返回
    analysisRDD.sortBy(_._2, false).take(10).map(_._1)

  }
}
