package org.fire.spark.streaming.core

import com.typesafe.config.{Config, ConfigFactory}


/**
  * Created by guoning on 16/8/24.
  *
  * 默认配置
  */
trait FireConfig {
  val config: Config = ConfigFactory.load()
}
