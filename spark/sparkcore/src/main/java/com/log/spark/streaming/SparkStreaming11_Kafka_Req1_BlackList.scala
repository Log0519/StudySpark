package com.log.spark.streaming

import com.log.spark.util.JDBCUtil
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object SparkStreaming11_Kafka_Req1_BlackList {

  def main(args: Array[String]): Unit = {
    //TODO 需求一 黑名单
    //TODO 实现实时的动态黑名单机制：将每天对某个广告点击超过 30 次的用户拉黑。
    //StreamingContext创建时需要传递两个参数，第一个参数表示环境配置，第二个表示批处理的周期
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

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


    val ds = adClickData.transform(
      rdd => {
        //TODO 通过JDBC周期性获得数据
        //TODO 周期性获取黑名单数据
        val blackList = ListBuffer[String]()

        val conn: Connection = JDBCUtil.getConnection
        val pstat: PreparedStatement = conn.prepareStatement("select userid from black_list")

        val rs: ResultSet = pstat.executeQuery()
        while (rs.next()) {
          blackList.append(rs.getString(1))
        }

        rs.close()
        pstat.close()
        conn.close()

        //TODO 判断点击用户是否在黑名单中
        val filterRDD: RDD[AdClickData] = rdd.filter(
          data => {
            !blackList.contains(data.user)
          }
        )
        //TODO 如果用户不在黑名单中，那么进行统计数量（每个周期）
        filterRDD.map(
          data => {
            val sdf = new SimpleDateFormat("yyyy-MM-dd")
            val day = sdf.format(new Date(data.ts.toLong))
            val user = data.user
            val ad = data.ad
            ((day, user, ad), 1)
          }
        ).reduceByKey(_ + _)

      }
    )


    ds.foreachRDD(
      rdd => {
        rdd.foreach {
          case ((day, user, ad), count) => {
            //TODO 添加时间戳
            println(s"${day} ${user} ${ad} ${count}")

            if (count >= 30) {
              //TODO 如果统计数量超过预值30，那么将用户拉入到黑名单
              val conn: Connection = JDBCUtil.getConnection
              val pstat: PreparedStatement = conn.prepareStatement(
                """
                  |insert into black_list (userid) values (?)
                  |on DUPLICATE KEY
                  |UPDATE userid=?
                  |""".stripMargin)

              pstat.setString(1, user)
              pstat.setString(2, user)
              pstat.executeUpdate()

              conn.close()
              pstat.close()
              pstat.close()
            } else {
              //TODO 如果没有超过预值，需要将当天的广告点击数量进行聚合更新
              val conn: Connection = JDBCUtil.getConnection
              val pstat: PreparedStatement = conn.prepareStatement(
                """
                  |select * from user_ad_count
                  |where dt =? and userid=? and adid=?
                  |""".stripMargin)
              pstat.setString(1, day)
              pstat.setString(2, user)
              pstat.setString(3, ad)
              val rs = pstat.executeQuery()
              //查询统计表数据
              if (rs.next()) {
                //如果存在数据，那么更新
                val pstat1 = conn.prepareStatement(
                  """
                    |update user_ad_count
                    |set count =count + ?
                    |where dt=? and userid=? and adid=?
                    |""".stripMargin)
                pstat1.setInt(1, count)
                pstat1.setString(2, day)
                pstat1.setString(3, user)
                pstat1.setString(4, ad)
                pstat1.executeUpdate()
                pstat1.close()
                //TODO 判断更新后的点击数据是否超过预值，如果超过把用户纳入黑名单
                val pstat2=conn.prepareStatement(
                  """
                    |select
                    | *
                    | from user_ad_count
                    | where dt=? and userid=? and adid=? and count>=30
                    |""".stripMargin)
                pstat2.setString(1, day)
                pstat2.setString(2, user)
                pstat2.setString(3, ad)
                val rs2: ResultSet = pstat2.executeQuery()
                if(rs2.next()){
                  val pstat3: PreparedStatement = conn.prepareStatement(
                  """
                    |insert into black_list (userid) values (?)
                    |on DUPLICATE KEY
                    |UPDATE userid=?
                    |""".stripMargin)

                  pstat3.setString(1, user)
                  pstat3.setString(2, user)
                  pstat3.executeUpdate()
                  pstat3.close()


                }

                rs2.close()
                pstat2.close()
              } else {
                //如果不存在数据，那么新增数据
                val pstat1: PreparedStatement = conn.prepareStatement(
                  """
                    |insert into user_ad_count(dt,userid,adid,count) values(?,?,?,?)
                    |""".stripMargin)

                pstat1.setString(1, day)
                pstat1.setString(2, user)
                pstat1.setString(3, ad)
                pstat1.setInt(4, count)
                pstat1.executeUpdate()

                pstat1.close()

              }

              rs.close()
              pstat.close()
              conn.close()


            }
          }
        }
      }
    )

    ssc.start()


    //2.等待采集器的关闭
    ssc.awaitTermination()


  }

  //广告点击数据
  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)

}
