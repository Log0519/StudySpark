package com.log.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming05_State {

  def main(args: Array[String]): Unit = {
    //TODO 创建环境对象
    //StreamingContext创建时需要传递两个参数，第一个参数表示环境配置，第二个表示批处理的周期
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))
            ssc.checkpoint("cp")
    //TODO 使用有状态的时候，需要设定检查点路径 ↑
    //无状态数据操作，只对采集周期内的数据进行处理
    //有些时候，需要保留数据统计结果（状态），实现数据的汇总，不能用reduceByKeY

    val datas: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val wordToOne: DStream[(String, Int)] = datas.map((_, 1))


   // val wordToCount: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)
      //updateStateByKey根据key对数据的状态进行更新
    //传递参数含有两个值，第一个表示相同key的value数据
    //第二个表示缓存区相同key的value数据
      val statue: DStream[(String, Int)] = wordToOne.updateStateByKey(
        (seq: Seq[Int], buff: Option[Int]) => {
          val newCount = buff.getOrElse(0) + seq.sum
          Option(newCount)
        }
      )

    statue.print()


    ssc.start()



    //2.等待采集器的关闭
    ssc.awaitTermination()



  }

}
