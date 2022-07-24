package com.log.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Serial {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Serial")
    val sc = new SparkContext(sparkConf)

    val rdd: RDD[String] = sc.makeRDD(Array("Hello world", "Hello spark", "hive", "atguigu"))

    val search = new Search("h")
    search.getMatch1(rdd).collect().foreach(println)

    sc.stop()
  }

  //查询对象
  //调用getMatch1类需要序列化，调用getMatch2不用
  //类的构造参数其实是类的属性，构造参数需要进行闭包检测，等同于类进行闭包检测，所以需要序列化
  class Search(query:String) extends Serializable {
    def isMatch(s: String): Boolean = {
      s.contains(query)
    }
    // 函数序列化案例
    def getMatch1 (rdd: RDD[String]): RDD[String] = {
      //rdd.filter(this.isMatch)
      rdd.filter(isMatch)
    }
    // 属性序列化案例
    def getMatch2(rdd: RDD[String]): RDD[String] = {
      //rdd.filter(x => x.contains(this.query)),等同于下面那句话
      rdd.filter(x => x.contains(query))
      //解决方案
      //q和类外参无关系，而且q可以序列化
      //val q = query
      //rdd.filter(x => x.contains(q))
    }
  }
}
