package com.log.spark.sql

import org.apache.spark.SparkConf

import org.apache.spark.sql._


object Spark04_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {
    //TODO 创建Spark的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = new SparkSession.Builder().config(sparkConf).getOrCreate()
    import spark.implicits._
    //读取sql的数据
    val df =  spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://127.0.0.1:3306/db1?useSSL=false")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "fuxiaoluo")
      .option("dbtable", "student")
      .load()
    df.show

    //保存数据
    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/spark_sql")
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("user", "root")
      .option("password", "fuxiaoluo")
      .option("dbtable", "student")
      .mode(SaveMode.Append)
      .save
    //TODO 关闭环境
    spark.close()
  }

}
