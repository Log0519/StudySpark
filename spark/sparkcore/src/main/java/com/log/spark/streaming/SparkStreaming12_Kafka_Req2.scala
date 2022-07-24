package com.log.spark.streaming

import com.log.spark.streaming.SparkStreaming11_Kafka_Req1_BlackList.AdClickData
import com.log.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.text.SimpleDateFormat
import java.util.Date

object SparkStreaming12_Kafka_Req2 {

  def main(args: Array[String]): Unit = {
    //TODO kafka接受数据 需求二
    //TODO 实时统计每天各地区各城市各广告的点击总流量，并将其存入 MySQL。
    //StreamingContext创建时需要传递两个参数，第一个参数表示环境配置，第二个表示批处理的周期
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf,Seconds(3))

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
    val reduceDS: DStream[((String, String, String, String), Int)] = adClickData.map(
      data => {
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val day = sdf.format(new Date(data.ts.toLong))
        val area = data.area
        val city = data.city
        val ad = data.ad
        ((day, area, city, ad), 1)
      }
    ).reduceByKey(_ + _)

    reduceDS.foreachRDD(
      rdd=>{
        rdd.foreachPartition(
          iter=>{
            val conn=JDBCUtil.getConnection
            val pstat=conn.prepareStatement(
              """
                |insert into area_city_ad_count(dt,area,city,adid,count)
                |values(?,?,?,?,?)
                |on DUPLICATE KEY
                |UPDATE count=count+?
                |""".stripMargin)
            iter.foreach {
              case ((day, area, city, ad), sum)=>{
                pstat.setString(1,day)
                pstat.setString(2,area)
                pstat.setString(3,city)
                pstat.setString(4,ad)
                pstat.setInt(5,sum)
                pstat.setInt(6,sum)
                pstat.executeUpdate()
              }
            }
            pstat.close()
            conn.close()
          }
        )
      }
    )


    ssc.start()


    //2.等待采集器的关闭
    ssc.awaitTermination()



  }
  //广告点击数据
  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)
}
