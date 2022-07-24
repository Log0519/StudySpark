package com.log.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Join {

  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext创建时需要传递两个参数，第一个参数表示环境配置，第二个表示批处理的周期
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")
    val data9999: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val data8888: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 8888)

    //所谓的DS的join操作其实就是两个RDD的join
    val map9999: DStream[(String, Int)] = data9999.map((_, 9))
    val map8888: DStream[(String, Int)] = data8888.map((_, 9))

    val joinDS: DStream[(String, (Int, Int))] = map9999.join(map8888)

    joinDS.print()



    ssc.start()



    //2.等待采集器的关闭
    ssc.awaitTermination()


  }

}
