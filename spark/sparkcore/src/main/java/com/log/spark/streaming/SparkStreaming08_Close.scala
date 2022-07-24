package com.log.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

import java.lang
import java.lang.Thread

object SparkStreaming08_Close {

  def main(args: Array[String]): Unit = {

    //TODO 线程的关闭
    //    val thread=new Thread()
    //    thread.start()
    //    thread.stop()强制关闭，数据会出现安全问题
    //TODO 创建环境对象
    //StreamingContext创建时需要传递两个参数，第一个参数表示环境配置，第二个表示批处理的周期
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))


    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))
    wordToOne.print()


    ssc.start() //TODO 应该在启动以后创建一个线程来关闭
    new Thread(
      new Runnable {
        override def run(): Unit = {
          //优雅的关闭,
          //第二个参数，计算节点不再接受新的数据，把当前的数据处理完后再关闭
          //需要在第三方程序中增加关闭状态！！！
          //Mysql:Table(stopSpark)=>Row=>data
          //Redis:Data(K-V)
          //ZK :/stopSpark
          //HDFS:/stopSpark
          //TODO 真正代码：
          //         while (true) {
          //            if (true) {
          //              //          获取SparkStreaming的状态
          //              val state: StreamingContextState = ssc.getState()
          //              if (state == StreamingContextState.ACTIVE) {
          //                ssc.stop(true, true)
          //              }
          //            }
          //            Thread.sleep(5000)
          //          }
          Thread.sleep(5000) //测试5s中后自动关闭
          val state: StreamingContextState = ssc.getState()
          if (state == StreamingContextState.ACTIVE) {
            ssc.stop(true, true)
          }
          System.exit(0)//线程停止，可以自动停，whileTrue的情况下要手动停
        }
      }
    ).start()

    //2.等待采集器的关闭
    ssc.awaitTermination() //问题：阻塞了main线程


  }

}
