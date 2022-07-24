package com.log.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object SparkStreaming02_Queue {

  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext创建时需要传递两个参数，第一个参数表示环境配置，第二个表示批处理的周期
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

    //TODO 逻辑处理
    val rddQueue =new mutable.Queue[RDD[Int]]()
    val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)
    val mappedStream = inputStream.map((_,1))
    val reduceStream: DStream[(Int, Int)] = mappedStream.reduceByKey(_ + _)
    reduceStream.print()


    //SparkStreaming采集器是长期执行的任务，不能直接关闭
    //main方法执行完毕应用程序就结束了，所以main方法不能执行完毕
    //1.启动采集器
    ssc.start()
    //8.循环创建并向 RDD 队列中放入 RDD
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }


    //2.等待采集器的关闭
    ssc.awaitTermination()


  }

}
