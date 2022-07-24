package com.log.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_Output1 {

  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext创建时需要传递两个参数，第一个参数表示环境配置，第二个表示批处理的周期
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp1")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    //TODO foreachRDD
    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

    val windowDS:DStream[(String,Int)] = wordToOne.reduceByKeyAndWindow(
      (x: Int, y: Int) => {
        x + y
      }, (x: Int, y: Int) => {
        x - y
      }, Seconds(9), Seconds(3)

    )
    //print会出现时间戳
    //foreachRDD不会出现时间戳
    //foreachRDD(func)：这是最通用的输出操作，即将函数 func 用于产生于 stream 的每一个
    //RDD。其中参数传入的函数 func 应该实现将每一个 RDD 中数据推送到外部系统，如将
    //RDD 存入文件或者通过网络将其写入数据库。
    //通用的输出操作 foreachRDD()，它用来对 DStream 中的 RDD 运行任意计算。这和 transform()
    //有些类似，都可以让我们访问任意 RDD。在 foreachRDD()中，可以重用我们在 Spark 中实现的
    //所有行动操作。比如，常见的用例之一是把数据写到诸如 MySQL 的外部数据库中。
    //注意：
    //1) 连接不能写在 driver 层面（序列化）
    //2) 如果写在 foreach 则每个 RDD 中的每一条数据都创建，得不偿失；
    //3) 增加 foreachPartition，在分区创建（获取）。
    //windowDS.foreachRDD(

    //)

    ssc.start()



    //2.等待采集器的关闭
    ssc.awaitTermination()


  }

}
