package com.log.spark.core.framework_controller_service_dao.service

import com.log.spark.core.framework_controller_service_dao.common.Trait_Service
import com.log.spark.core.framework_controller_service_dao.dao.WordCountDao
import org.apache.spark.rdd.RDD

//服务层
class WordCountService extends Trait_Service {
  private val wordCountDao = new WordCountDao

  //数据分析
def dataAnalysis()={
  val lines: RDD[String] = wordCountDao.readFile("datas/word")

  val words = lines.flatMap(_.split(" "))
  val wordToOne = words.map(word => (word, 1))
  val wordToCount = wordToOne.reduceByKey(_ + _)
  val array:Array[(String,Int)]=wordToCount.collect()
  array
}
}
