package com.log.spark.core.rdd.operator.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_RDD_Operator_Action {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    //TODO 行动算子 -相关
    val rdd: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    val user = new User
rdd.foreach(
  num=> {
    println("age" + (user.age + num))
  }
    //User需要序列化 extends Serializable ,或者前面加一个case，后面再加一个（）
  //样例类会自动混入序列化特质（实现可序列化接口）
  //及时List中没有数据，但是依旧需要序列化，因为
  //RDD算子中传递的函数包含闭包操作，会进行检测功能，称为闭包检测
)

    sc.stop()

  }
  class User extends Serializable {
    var age:Int=30
  }
}
