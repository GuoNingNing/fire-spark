package org.apache.spark


import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerApplicationStart}

/**
  * Created by cloud on 18/1/19.
  */
class StartSparkAppListener(val sparkConf: SparkConf) extends SparkListener with Logging{

  private def x(appId : String): Unit = {
    logInfo(appId)
  }

  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    val appId = applicationStart.appId
    x(appId.get)
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    x(applicationEnd.time.toString)
  }

}
