package com.log.spark.sql

import org.apache.parquet.filter2.predicate.Operators.UserDefined
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object Spark02_SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    //TODO 创建Spark的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = new SparkSession.Builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    val df: DataFrame = spark.read.json("datas/user.json")
    df.createOrReplaceTempView("user")
    spark.udf.register("prefixName",(name:String)=>{
      "Name: "+name
    })

    spark.sql("select age,prefixName(username) from user").show()


    //TODO 关闭环境
    spark.close()
  }
  case class User(id:Int,name:String,age:Int)

}
