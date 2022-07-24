package com.log.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._


object Spark05_SparkSQL_Hive {
  def main(args: Array[String]): Unit = {
    //TODO 创建Spark的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = new SparkSession.Builder().config(sparkConf).getOrCreate()

    //使用SparkSQL连接外置Hive
    //1.拷贝Hive-size.xml文件到classpath（resources）下
//2.启动Hive支持
    //3.增加对应的依赖关系（包含mysql驱动、）
    spark.sql("show tables").show



    //TODO 关闭环境
    spark.close()
  }

}
