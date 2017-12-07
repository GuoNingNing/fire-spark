package org.fire.spark.streaming.core

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
  * Created by guoning on 16/6/15.
  *
  * SQLContext 单例
  */
object SQLContextSingleton {

  @transient private var instance: SQLContext = _
  @transient private var hiveContext: SQLContext = _

  def getInstance(@transient sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }

  /**
    * 获取 HiveContext
    *
    * @param sparkContext
    * @return
    */
  def getHiveContext(@transient sparkContext: SparkContext): SQLContext = {
    if (hiveContext == null) {
      hiveContext = new org.apache.spark.sql.hive.HiveContext(sparkContext)
    }
    hiveContext
  }
}
