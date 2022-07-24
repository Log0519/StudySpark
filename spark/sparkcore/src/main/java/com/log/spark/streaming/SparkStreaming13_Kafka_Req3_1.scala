package com.log.spark.streaming

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.{File, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object SparkStreaming13_Kafka_Req3_1 {

  def main(args: Array[String]): Unit = {
    //TODO kafka接受数据 需求一
    //TODO 最近一小时广告点击量， 可视化！！！！
    //StreamingContext创建时需要传递两个参数，第一个参数表示环境配置，第二个表示批处理的周期
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(5))

    //TODO 连接 Kafka
    val kafkaPara: Map[String, Object] = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "log",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
    )

    val kafkaDataDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("atguiguNew"), kafkaPara)
    )

    val adClickData: DStream[AdClickData] = kafkaDataDS.map(
      kafkaData => {
        val data: String = kafkaData.value()
        val datas: Array[String] = data.split(" ")
        AdClickData(datas(0), datas(1), datas(2), datas(3), datas(4))
      }
    )

    //最近一分钟,每10s计算一次
    //12:11=>12:10,12:19=>12:10,12:59=>12:50
    //55/10*10=50
    //32/10*10=30

    //设计窗口的计算
    val reduceDS: DStream[(Long, Int)] = adClickData.map(
      data => {
        val ts: Long = data.ts.toLong
        val newTS = ts / 10000 * 10000
        (newTS, 1)
      }
    ).reduceByKeyAndWindow((x:Int,y:Int)=>{x+ y}, Seconds(60), Seconds(10))

    //reduceDS.print()
    //TODO 可视化
    reduceDS.foreachRDD(
      rdd=>{
        val list=ListBuffer[String]()

        val datas: Array[(Long, Int)] = rdd.sortByKey(true).collect()
        datas.foreach{
          case(time,cnt)=>{
            val timeString: String = new SimpleDateFormat("mm:ss").format(new Date(time.toLong))

            list.append(s"""{"xtime":"${timeString}","yval":"${cnt}"}""".stripMargin)
          }
        }


        //输出文件
        val out=new PrintWriter(new FileWriter(new File("G:\\IDEA_Spark\\spark\\datas\\adclick\\adclick.json")))
      out.print("["+list.mkString(",")+"]")
        out.flush()
        out.close()
      }
    )

    ssc.start()


    //2.等待采集器的关闭
    ssc.awaitTermination()



  }
  //广告点击数据
  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)
}
