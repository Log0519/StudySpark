package com.log.spark.core.framework_controller_service_dao.application

import com.log.spark.core.framework_controller_service_dao.common.Trait_Application
import com.log.spark.core.framework_controller_service_dao.controller.WordCountController


object WordCountApplication extends App with Trait_Application{

  //启动应用程序,初始化
  isZero (){
    val wordCountController = new WordCountController
    wordCountController.dispatch()//控制抽象
  }



}
