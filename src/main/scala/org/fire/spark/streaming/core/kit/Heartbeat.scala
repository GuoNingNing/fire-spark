package org.fire.spark.streaming.core.kit

import java.util.concurrent.{Executors, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.streaming.StreamingContext
import org.fire.spark.streaming.core.Logging

import scala.util.Try

/**
  * Created by cloud on 18/4/12.
  */
class Heartbeat(private val ssc: StreamingContext) extends Logging {
  private val threadFactory =
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("spark-monitor-heartbeat-thread").build()
  private val heartbeatExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory)

  def start(): Unit = {
    val sparkConf = ssc.sparkContext.getConf

    sparkConf.get("spark.monitor.heartbeat.api", "none") match {
      case "none" =>
      case heartbeat =>

        val initialDelay = sparkConf.get("spark.monitor.heartbeat.initialDelay", "60000").toLong
        val period = sparkConf.get("spark.monitor.heartbeat.period", "10000").toLong

        heartbeatExecutor.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = {
            Try {
              logInfo(s"send heartbeat to $heartbeat/${sparkConf.getAppId}/${period * 1.5} ")
              Utils.httpGet(s"$heartbeat/${sparkConf.getAppId}/${period * 3}",Seq.empty[(String,String)])
            }
          }
        }, initialDelay, period, TimeUnit.MILLISECONDS)
    }
  }

  def stop(): Unit = {
    logInfo("shutdown heartbeatExecutor ...")
    heartbeatExecutor.shutdown()
  }

}
