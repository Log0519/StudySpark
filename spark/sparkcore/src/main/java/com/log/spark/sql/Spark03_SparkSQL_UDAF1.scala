package com.log.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession, functions}


object Spark03_SparkSQL_UDAF1 {
  def main(args: Array[String]): Unit = {
    //TODO 创建Spark的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = new SparkSession.Builder().config(sparkConf).getOrCreate()
    val df: DataFrame = spark.read.json("datas/user.json")
    //早期版本中，Spark不能在sql中使用强类型UDAF操作
    //SQL,DSL,
    //import spark.implicits._
    //早期UDAF强类型聚合函数，使用DSL语法操作
    //case class User(username:String,age:Long)
    //val ds:DataSet[User]=df.as[User]
//将UDAF函数转化为查询的列对象
   //val usafCol:TypedColumn[User,Long]=new  myavgUDAF().toColumn
    //ds.select(usafCol)

    df.createOrReplaceTempView("user")

    spark.udf.register("ageAvg",functions.udaf(new myavgUDAF()))

    spark.sql("select ageAvg(age) from user" ).show()

    //TODO 关闭环境
    spark.close()
  }
  case class User(id:Int,name:String,age:Int)


 case class Buff(var total:Long,var count:Long)
  //自定义聚合函数：计算年龄的平均值
  //TODO 1.继承org.apache.spark.sql.expressions.Aggregator,定义泛型
  //IN:输入的数据类型
  //BUF:
  //OUT:
  class myavgUDAF extends Aggregator[Long,Buff,Long]{//第一个换成User
   //缓冲区初始化
    override def zero: Buff ={
      Buff(0L,0L)
    }
//根据输入的数据更新缓冲区的数据
    override def reduce(buff: Buff, in: Long): Buff = {//第二个换成User类型
      buff.total=buff.total+in//in.age
      buff.count=buff.count+1
      buff
    }
//合并缓冲区
    override def merge(buff1: Buff, buff2: Buff): Buff = {
      buff1.total=buff1.total+buff2.total
      buff1.count=buff1.count+buff2.count
      buff1
    }
//计算结果
    override def finish(buff: Buff): Long = {
      buff.total/buff.count
    }
//缓冲区编码操作
    override def bufferEncoder: Encoder[Buff] =Encoders.product
//输出的编码操作
    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }


}
