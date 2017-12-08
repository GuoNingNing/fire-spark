package org.fire.spark.streaming.core

import org.fire.spark.streaming.core.kit.Utils

import scala.collection.Map
import scala.util.Try


/**
  * Created by guoning on 16/8/24.
  *
  * 默认配置
  */
trait FireConfig {
  val config: Map[String, String] = Try {
    Utils.getPropertiesFromFile("application.properties")
  } getOrElse Map.empty[String, String]
}
