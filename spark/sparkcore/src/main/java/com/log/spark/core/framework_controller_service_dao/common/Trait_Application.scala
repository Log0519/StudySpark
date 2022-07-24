package com.log.spark.core.framework_controller_service_dao.common

import com.log.spark.core.framework_controller_service_dao.controller.WordCountController
import com.log.spark.core.framework_controller_service_dao.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

//特质
trait Trait_Application {

  //控制抽象op
  //设置默认master，app
  def isZero(master:String="local[*]",app:String="Application")(op: =>scala.Unit):Unit={
    //TODO 建立和spark的链接
    val sparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparkConf)
    EnvUtil.put(sc)
    //执行逻辑
    try {
      op
    }catch {
      case ex=>println(ex.getMessage)
    }

    //关闭连接
    sc.stop()
    EnvUtil.clear()
  }

}
