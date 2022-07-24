package com.log.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_RDD_Req {
  def main(args: Array[String]): Unit = {
    //配置
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Req")
    val sc = new SparkContext(sparkConf)
    //TODO 案例实操
    //TODO 1) 数据准备
    //TODO agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
    //TODO 2) 需求描述
    //TODO 统计出每一个省份每个广告被点击数量排行的 Top3
    //TODO 3) 需求分析
    //TODO 4) 功能实现

    //1.获取原始数据:时间戳，省份，城市，用户，广告
    val datardd: RDD[String] = sc.textFile("datas/agent.log")

    //2.将原始数据进行结构转换，方便统计
    //时间戳，省份，城市，用户，广告=>((省份，广告),1)
    val mapRDD: RDD[((String, String), Int)] = datardd.map(
      line => {
        val datas: Array[String] = line.split(" ")
        ((datas(1), datas(4)), 1)
      }
    )

    //3.将转换结构后的数据，进行分组聚合=>((省份，广告),sum)
    val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)

    //4.将聚合结果进行结构的转换((省份，广告),sum)=>(省份,(广告,sum))
    val newMapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
      case ((prv, ad), sum) => (prv, (ad, sum))//模式匹配
    }

    //5.将转换结构后的数据，根据省份进行分组
    // (省份,((广告a,suma),(广告b,sumb)...)
    val groupRDD: RDD[(String, Iterable[(String, Int)])] = newMapRDD.groupByKey()

    //6.将分组后的数据组内排序，降序，取前三名
    val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
      iter => {
        //降序排列，先把迭代器转化为可排序的List,scala默认是升序，要加一个参数(Ordering.Int.reverse)
        iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3)
      }
    )

    //7.采集数据，打印
resultRDD.collect().foreach(println)
sc.stop()

  }
}
