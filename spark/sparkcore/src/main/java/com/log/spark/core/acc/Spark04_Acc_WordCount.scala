package com.log.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable


object Spark04_Acc_WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
    val sc = new SparkContext(sparkConf)
    // TODO 累加器 -Acc 分布式共享只写变量
    val rdd= sc.makeRDD(List("Hello","Spark", "Hello"))
    //可以省去shuffle过程
    //自定义累加器
    //1.创建累加器对象
    val wcAcc = new MyAcc()
    //2.向spark进行注册
    sc.register(wcAcc,"wordCountAcc")

    rdd.foreach(word => wcAcc.add(word))//使用累加器
    //获取累加器结果
println(wcAcc.value)

    sc.stop()
  }

  /*自定义累加器：WordCount
  1.继承org.apache.spark.util.AccumulatorV2
  两个泛型：IN：累加器输入的数据类型 String
          OUT:累加器返回的数据类型 mutable.Map[String, Long]
   */
  class MyAcc extends org.apache.spark.util.AccumulatorV2[String, mutable.Map[String, Long]] {
    private var wcMap=mutable.Map[String,Long]()

    //判断是否为初始状态
    override def isZero: Boolean = {
      wcMap.isEmpty
    }
    //复制一个累加器
    override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = {
      new MyAcc()
    }

    //重置累加器
    override def reset(): Unit = {
      wcMap.clear()
    }

    //获取累加器需要计算的值
    override def add(word: String): Unit = {
      val newCount: Long = wcMap.getOrElse(word, 0L) + 1
      wcMap.update(word,newCount)
    }

    //Driver合并多个累加器
    override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = {
      val map1=this.wcMap
      val map2=other.value
      map2.foreach{
        case (word,count) =>{
          val newCount: Long = map1.getOrElse(word,0L)+count
          map1.update(word,newCount)
        }
      }
    }

    //累加器结果
    override def value: mutable.Map[String, Long] = {
      wcMap
    }
  }
}