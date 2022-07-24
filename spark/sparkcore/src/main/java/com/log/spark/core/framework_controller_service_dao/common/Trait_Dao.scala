package com.log.spark.core.framework_controller_service_dao.common

import com.log.spark.core.framework_controller_service_dao.util.EnvUtil

//特质
trait Trait_Dao {

  def readFile(path:String)={
    EnvUtil.take().textFile(path)
  }

}
