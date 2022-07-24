package com.log.spark.core.framework_controller_service_dao.controller


import com.log.spark.core.framework_controller_service_dao.common.Trait_Controller
import com.log.spark.core.framework_controller_service_dao.service.WordCountService

//控制层
class WordCountController extends Trait_Controller{
  private val wordCountService = new WordCountService
  //dispatch=调度
  def dispatch():Unit={
    //TODO 执行业务操作
    val array= wordCountService.dataAnalysis()
    array.foreach(println)

  }

}
