package org.fire.spark.streaming.core

import java.io.File
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.streaming.{CongestionMonitorListener, JobInfoReportListener, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.fire.spark.streaming.core.kit.Utils
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.util.Try
import sys.process._

/**
  * Created by guoning on 2017/4/26.
  *
  * Spark Streaming 入口封装
  *
  */
trait FireStreaming {

  lazy val logger = LoggerFactory.getLogger(getClass)

  protected final def args: Array[String] = _args

  private final var _args: Array[String] = _

  private val startListeners = new ArrayBuffer[String]()


  // 是否开启监控
  private var monitor: Boolean = false

  // checkpoint目录
  private var checkpointPath: String = ""

  // 从checkpoint 中恢复失败，则重新创建
  private var createOnError: Boolean = true

  /**
    * 初始化，函数，可以设置 sparkConf
    *
    * @param sparkConf
    */
  def init(sparkConf: SparkConf): Unit = {}

  /**
    * spark 启动后 调用
    */
  def afterStarted(ssc: StreamingContext): Unit = {}

  def addAllEventListeners(l: String): Unit = startListeners += l

  /**
    * 处理函数
    *
    * @param ssc
    */
  def handle(ssc: StreamingContext): Unit

  private var heartbeatExecutor: ScheduledThreadPoolExecutor = null

  /**
    * 心跳检测程序
    *
    * @param ssc
    */
  def heartbeat(ssc: StreamingContext): Unit = {
    val sparkConf = ssc.sparkContext.getConf

    sparkConf.get("spark.monitor.heartbeat.api", "none") match {
      case "none" =>
      case heartbeat =>

        val initialDelay = sparkConf.get("spark.monitor.heartbeat.initialDelay", "60000").toLong
        val period = sparkConf.get("spark.monitor.heartbeat.period", "10000").toLong

        val threadFactory = new ThreadFactoryBuilder().setDaemon(true).setNameFormat("spark-monitor-heartbeat-thread").build()
        val heartbeatExecutor = new ScheduledThreadPoolExecutor(1, threadFactory)
        heartbeatExecutor.setRemoveOnCancelPolicy(true)
        heartbeatExecutor.scheduleAtFixedRate(new Runnable {
          override def run(): Unit = {

            Try {
              logger.info(s"send heartbeat to $heartbeat/${sparkConf.getAppId}/${period * 1.5} ")
              s"curl $heartbeat/${sparkConf.getAppId}/${period * 3} " #>> new File("/dev/null") !!
            }

          }
        }, initialDelay, period, TimeUnit.MILLISECONDS)
    }
  }


  /**
    * 创建 Context
    *
    * @return
    */
  def creatingContext(): StreamingContext = {

    val sparkConf = new SparkConf()


    // 约定传入此参数,则表示本地 Debug
    if (sparkConf.contains("spark.properties.file")) {
      sparkConf.setAll(Utils.getPropertiesFromFile(sparkConf.get("spark.properties.file")))
      sparkConf.setAppName("LocalDebug").setMaster("local[*]")
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10")
    }
    // 如果有心跳监控，则提交成功后退出终端
    if(sparkConf.contains("spark.monitor.heartbeat.api")){
      sparkConf.set("spark.yarn.submit.waitAppCompletion","false")
    }

    init(sparkConf)
    //    addAllEventListeners("org.apache.spark.StartSparkAppListener")

    val extraListeners = startListeners.mkString(",") + "," + sparkConf.get("spark.extraListeners", "")
    if (extraListeners != "") sparkConf.set("spark.extraListeners", extraListeners)

    // 时间间隔
    val slide = sparkConf.get("spark.batch.duration").toInt
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(slide))
    handle(ssc)
    ssc
  }


  private def printUsageAndExit(): Unit = {

    System.err.println(
      """
        |"Usage: FireStreaming [options]
        |
        | Options are:
        |   --checkpointPath <checkpoint 目录设置>
        |   --createOnError <从 checkpoint 恢复失败,是否重新创建 true|false>
        |""".stripMargin)
    System.exit(1)
  }


  def main(args: Array[String]): Unit = {

    this._args = args

    var argv = args.toList

    while (argv.nonEmpty) {
      argv match {
        case ("--checkpointPath") :: value :: tail =>
          checkpointPath = value
          argv = tail
        case ("--createOnError") :: value :: tail =>
          createOnError = value.toBoolean
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          printUsageAndExit()
      }
    }


    val context = checkpointPath match {
      case "" => creatingContext()
      case ck =>
        val ssc = StreamingContext.getOrCreate(ck, creatingContext, createOnError = createOnError)
        ssc.checkpoint(ck)
        ssc
    }

    context.start()
    afterStarted(context)
    heartbeat(context)
    context.awaitTermination()

    if (heartbeatExecutor != null) {
      logger.info("shutdown heartbeatExecutor ...")
      heartbeatExecutor.shutdown()
    }
  }
}
