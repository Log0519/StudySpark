package com.log.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}

import scala.collection.mutable

object Spark04_Req1_HotCategoryTop10Analysis_better3 {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
    val sc = new SparkContext(sparkConf)


    //TODO 需求 1：Top10 热门品类  优化版本
    //TODO 问题：1.actionRDD重复使用,已解决
    //          2.cogroup存在shuffle，性能可能较低，已解决
    //          3.大量的reduceBykey，大量的shuffle，如果是相同的数据源，spark提供了优化、缓存，
    //          但是这里是不相同的数据源,已解决
    //          4.完全去除reduceByKey，使用累加器,无shuffle，性能最好,该版本

    //1.读取原始的日志数据
    val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
    //TODO 优化问题 3 只有一个reduceByKey
    //声明累加器
    val acc=new HotCategoryAccumulator
    sc.register(acc,"hotCategory")
    //2.将数据转换结构

    actionRDD.foreach(
      action => {
        val datas: Array[String] = action.split("_")
        if (datas(6) != "-1") {
          //点击的场合
         acc.add((datas(6),"click"))
        }
        else if (datas(8) != "null") {
          //下单的场合
          val ids: Array[String] = datas(8).split(",")
          ids.foreach(
            id=>{
              acc.add(id,"order")
            }
          )
        } else if (datas(10) != "null") {
          //支付的场合
          val ids: Array[String] = datas(10).split(",")
          ids.foreach(
            id=>{
              acc.add(id,"pay")
            }
          )
        }
      }
    )

    val accValue: mutable.Map[String, HotCategory] = acc.value
    val categoris: mutable.Iterable[HotCategory] = accValue.map(_._2)
//自定义排序
val sortRDD: List[HotCategory] = categoris.toList.sortWith(
  (left, rigeht) => {
    if (left.clickCnt > rigeht.clickCnt) {
      true
    }
    else if (left.clickCnt == rigeht.clickCnt) {
      if (left.orderCnt > rigeht.orderCnt) {
        true
      } else if (left.orderCnt == rigeht.orderCnt) {
        left.payCnt > rigeht.payCnt
      } else {
        false
      }
    } else {
      false
    }
  }
)
    //5.采集打印
    sortRDD.take(10).foreach(println)
    sc.stop()

  }

  case class HotCategory(cid: String, var clickCnt: Int, var orderCnt: Int, var payCnt: Int)
  //自定义累加器
  //1.继承AccumulatorV2
  //2.定义IN，OUT泛型
  //IN：（品类ID，行为类型）
  //OUT：mutable.Map[String,HotCategory]

  class HotCategoryAccumulator extends AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] {

    private val hcMap: mutable.Map[String, HotCategory] = mutable.Map[String, HotCategory]()

    override def isZero: Boolean = {
      hcMap.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, HotCategory]] = {
      new HotCategoryAccumulator()
    }

    override def reset(): Unit = {
      hcMap.clear()
    }

    override def add(v: (String, String)): Unit = {
      val cid: String = v._1
      val actionType: String = v._2
      val category: HotCategory = hcMap.getOrElse(cid, HotCategory(cid, 0, 0, 0))
      if (actionType == "click") {
        category.clickCnt += 1
      } else if (actionType == "order") {
        category.orderCnt += 1
      } else if (actionType == "pay") {
        category.payCnt += 1
      }
      hcMap.update(cid, category)
    }

      override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, HotCategory]]): Unit = {
        //两个map合并
        val map1: mutable.Map[String, HotCategory] = this.hcMap
        val map2: mutable.Map[String, HotCategory] = other.value

        map2.foreach {
          case (cid, hc) => {
            val category: HotCategory = map1.getOrElse(cid, HotCategory(cid, 0, 0, 0))
            category.clickCnt += hc.clickCnt
            category.orderCnt += hc.orderCnt
            category.payCnt += hc.payCnt
            map1.update(cid, category)
          }
        }
      }

    override def value: mutable.Map[String, HotCategory] = hcMap
  }


}
