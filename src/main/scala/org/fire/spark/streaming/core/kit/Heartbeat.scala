package org.fire.spark.streaming.core.kit

import java.io.File
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.streaming.StreamingContext
import org.fire.spark.streaming.core.Logging

import scala.util.Try
import scala.sys.process._

/**
  * Created by cloud on 18/4/13.
  */
class Heartbeat(private val ssc: StreamingContext) extends Logging {

  private val threadFactory =
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("spark-monitor-heartbeat-thread").build()
  private val heartbeatExecutor: ScheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(1,threadFactory)

  def start(): Unit = {
    val sparkConf = ssc.sparkContext.getConf

    sparkConf.get("spark.monitor.heartbeat.api", "none") match {
      case "none" =>
      case heartbeat =>

        val initialDelay = sparkConf.get("spark.monitor.heartbeat.initialDelay", "60000").toLong
        val period = sparkConf.get("spark.monitor.heartbeat.period", "10000").toLong

        heartbeatExecutor.setRemoveOnCancelPolicy(true)
        heartbeatExecutor.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = {

            Try {
              logInfo(s"send heartbeat to $heartbeat/${sparkConf.getAppId}/${period * 1.5} ")
              s"curl $heartbeat/${sparkConf.getAppId}/${period * 3} " #>> new File("/dev/null") !!
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
