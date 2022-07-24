package com.log.spark.sql

import com.log.spark.core.rdd.operator.action.Spark07_RDD_Operator_Action.User
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    //TODO 创建Spark的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = new SparkSession.Builder().config(sparkConf).getOrCreate()

    //TODO 执行逻辑
    //RDD

    //DataFrame
    val df: DataFrame = spark.read.json("datas/user.json")
    df.show()

    //DataFrame=>SQL
    //View可查不可改，table可查可改
    df.createOrReplaceTempView(("user"))
    spark.sql("select * from user").show()
    spark.sql("select age from user").show()
    spark.sql("select avg(age) from user").show()

    //DataFrame=>DSL,不用建表
    //在使用dataFrame时，如果设计到转换操作，需要引入转换规则
    import spark.implicits._ //spark不是包名，是变量或者对象的名称，是上面构建环境的对象
    df.select("age", "username").show()
    df.select($"age" + 1).show()
    df.select('age + 1).show()

    //DataSet
    // DataFrame是特定的DataSet,DataFrame的方法DataSet都能用
    val seq = Seq(1, 2, 3, 4)
    val ds: Dataset[Int] = seq.toDS()
    ds.show()


    //1. RDD<=>DataFrame
    //1.1 RDD->DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List(
      (1, "zhangsan", 30), (2, "lisi", 40)
    ))
    val df1: DataFrame = rdd.toDF("id", "name", "age")
    //1.2 DataFrame->RDD
    val rdd1: RDD[Row] = df1.rdd


    //2. DataFrame<=>DataSet
    //2.1 DataFrame->DataSet
    val ds1: Dataset[User] = df1.as[User]
    //2.2DataSet->DataFrame
    val df2: DataFrame = ds1.toDF()

    //3. RDD<=>DataSet
    //3.1 RDD->DataSet
    val ds2: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val rdd2: RDD[User] = ds2.rdd


    //TODO 关闭环境
    spark.close()
  }
  case class User(id:Int,name:String,age:Int)
}
