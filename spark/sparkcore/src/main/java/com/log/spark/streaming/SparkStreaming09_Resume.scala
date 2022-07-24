package com.log.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming09_Resume {

  def main(args: Array[String]): Unit = {

    //TODO 关闭以后，需要恢复数据，从检查点恢复数据,第二个参数，如果找不到恢复的，就构造环境对象
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("cp", () => {
      //TODO 创建环境对象
      //StreamingContext创建时需要传递两个参数，第一个参数表示环境配置，第二个表示批处理的周期
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
      val ssc = new StreamingContext(sparkConf, Seconds(3))


      val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
      val wordToOne: DStream[(String, Int)] = lines.map((_, 1))
      wordToOne.print()
      ssc
    })

    ssc.checkpoint("cp")

    ssc.start()

    ssc.awaitTermination() //问题：阻塞了main线程


  }

}
