package com.log.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Window {

  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext创建时需要传递两个参数，第一个参数表示环境配置，第二个表示批处理的周期
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    //TODO Window 多个采集周期当成一个整体
    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

    //窗口的范围应该是采集周期的整数倍
    //窗口可以滑动的，默认滑动步长是一个一个采集周期滑动，可能出现重复计算
    //避免这种情况，第二个参数可以设置滑动步长
    val windowDS: DStream[(String, Int)] = wordToOne.window(Seconds(6),Seconds(6))

    val wordToCount: DStream[(String, Int)] = windowDS.reduceByKey(_ + _)

    wordToCount.print()
    ssc.start()



    //2.等待采集器的关闭
    ssc.awaitTermination()


  }

}
