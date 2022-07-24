package com.log.spark.streaming
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.Random

object SparkStreaming10_MockData {

  def main(args: Array[String]): Unit = {
    //TODO 生成案例模拟数据 需求一
    //格式：timestamp area city userid adid
    //含义：时间戳 区域 城市 用户 广告

    //Application生成=>Kafka=>SparkStreaming=>Analysis
    val prop = new Properties()
    //创建kafka的生产者
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](prop)

  while (true){
    mockdata().foreach(
      data=>{
        //向kafka中生成数据
        val record = new ProducerRecord[String, String]("atguiguNew",data)
        producer.send(record)
        println(data)

      }
    )
    Thread.sleep(2000)
  }

def mockdata(): ListBuffer[String]={
  val list =ListBuffer[String]()
  val areaList =ListBuffer[String]("华北","华南","华东")
  val cityList =ListBuffer[String]("北京","上海","深圳")
  for(i <- 1 to Random.nextInt(50)){
    val area=areaList(new Random().nextInt(3))
    val city=cityList(new Random().nextInt(3))
    val userid=new Random().nextInt(6)+1
    val acid=new Random().nextInt(6)+1
    list.append(s"${System.currentTimeMillis()} ${area} ${city} ${userid} ${acid}")
  }
  list
}


  }
}
