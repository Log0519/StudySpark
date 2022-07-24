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

object SparkStreaming11_Kafka_Req1_BlackList_BetterIdea {

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
        //foreach这个方法会每个方法创建连接
        //foreach是RDD的算子，算子以外在Driver端执行，内在Executor执行，涉及闭包操作
        //那么需要把数据传输过去，数据需要序列化
        //但是数据库的连接对象是不能序列化的，连接不能写在driver层面

        //RDD提供了一个算子rdd.foreachPartition()可以有效提高效率
        //可以一个分区创建一个连接对象，大幅度减少连接对象的数量，提升效率
//        rdd.foreachPartition(
//          iter => {
//            val conn: Connection = JDBCUtil.getConnection
//            iter.foreach {
//              case ((day, user, ad), count)
//              => {
//        。。。。。。。
//              }
//            }
//            conn.close()
//          }
//        )
        rdd.foreach {
          case ((day, user, ad), count) => {
            //TODO 添加时间戳
            println(s"${day} ${user} ${ad} ${count}")

            if (count >= 30) {
              //TODO 如果统计数量超过预值30，那么将用户拉入到黑名单
              val conn: Connection = JDBCUtil.getConnection
              val sql =
                """
                  |insert into black_list (userid) values (?)
                  |on DUPLICATE KEY
                  |UPDATE userid=?
                  |""".stripMargin
              JDBCUtil.executeUpdate(conn, sql, Array(user, user))
              conn.close()

            } else {
              //TODO 如果没有超过预值，需要将当天的广告点击数量进行聚合更新
              val conn: Connection = JDBCUtil.getConnection
              val sql =
                """
                  |select * from user_ad_count
                  |where dt =? and userid=? and adid=?
                  |""".stripMargin
              val flg = JDBCUtil.isExist(conn, sql, Array(day, user, ad))

              //查询统计表数据
              if (flg) {
                //如果存在数据，那么更新
                val sql1 =
                  """
                    |update user_ad_count
                    |set count =count + ?
                    |where dt=? and userid=? and adid=?
                    |""".stripMargin
                JDBCUtil.executeUpdate(conn, sql1, Array(count, day, user, ad))

                //TODO 判断更新后的点击数据是否超过预值，如果超过把用户纳入黑名单
                val sql2 =
                  """
                    |select * from user_ad_count
                    |where dt=? and userid=? and adid=? and count>=30
                    |""".stripMargin
                val flg2 = JDBCUtil.isExist(conn, sql2, Array(day, user, ad))


                if (flg2) {
                  val sql3 =
                    """
                      |insert into black_list (userid) values (?)
                      |on DUPLICATE KEY
                      |UPDATE userid=?
                      |""".stripMargin
                  JDBCUtil.executeUpdate(conn, sql3, Array(user, user))
                }
              } else {
                //如果不存在数据，那么新增数据
                val sql4 =
                  """
                   insert into user_ad_count(dt,userid,adid,count) values(?,?,?,?)
                    |""".stripMargin
                JDBCUtil.executeUpdate(conn, sql4, Array(day, user, ad, count))
              }
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
