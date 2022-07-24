package com.log.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Window1 {

  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext创建时需要传递两个参数，第一个参数表示环境配置，第二个表示批处理的周期
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp1")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

    //当窗口范围比较大，但是滑动幅度比较小，那么他可以采用增加数据和删除数据的方式，无需重复计算
    //提升性能
    val windowDS:DStream[(String,Int)] = wordToOne.reduceByKeyAndWindow(
      (x: Int, y: Int) => {
        x + y
      }, (x: Int, y: Int) => {
        x - y
      }, Seconds(9), Seconds(3)

    )


   windowDS.print()
    ssc.start()



    //2.等待采集器的关闭
    ssc.awaitTermination()


  }

}
