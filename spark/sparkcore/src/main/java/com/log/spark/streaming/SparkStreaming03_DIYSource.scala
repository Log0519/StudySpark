package com.log.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable
import scala.util.Random

object SparkStreaming03_DIYSource {

  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext创建时需要传递两个参数，第一个参数表示环境配置，第二个表示批处理的周期
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //TODO 逻辑处理


    //SparkStreaming采集器是长期执行的任务，不能直接关闭
    //main方法执行完毕应用程序就结束了，所以main方法不能执行完毕
    //1.启动采集器

    val messageDS: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver)
    messageDS.print()
    ssc.start()


    //2.等待采集器的关闭
    ssc.awaitTermination()


  }
  //自定义数据采集器
  //1.继承Receiver,定义泛型
  //2.重写方法
  class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
    private var flg =true
    override def onStart(): Unit = {
      new Thread(new Runnable {
        override def run(): Unit = {
          while (flg){

            val message="采集的数据为： "+new Random().nextInt(10).toString
            store(message)//封装存储

            Thread.sleep(500)
          }
        }
      }).start()

    }

    override def onStop(): Unit = {
      flg=false



    }
  }

}
