package com.log.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Transform {

  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext创建时需要传递两个参数，第一个参数表示环境配置，第二个表示批处理的周期
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)



    // transform将底层RDD获取到后进行操作
    //使用场景：
    //1.DStream功能不完善
    //2.需要代码周期性的执行
    //区别在于写代码的位置,
    //Driver端
    val newDS: DStream[String] = lines.transform(
      rdd=>{
        //Driver端（周期性执行）
        rdd.map(
          str=>{
            //Executor端
            str
          }
        )
      }
    )
//Driver端
    val newDS1: DStream[String] = lines.map(
      datas => {
        //Executor端
        datas
      }
    )




    ssc.start()



    //2.等待采集器的关闭
    ssc.awaitTermination()


  }

}
