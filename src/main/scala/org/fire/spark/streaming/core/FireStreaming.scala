package org.fire.spark.streaming.core

import java.io.File
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.apache.spark.streaming.{CongestionMonitorListener, JobInfoReportListener, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.fire.spark.streaming.core.kit.{Heartbeat, Utils}
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

  private val sparkListeners = new ArrayBuffer[String]()


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

  /**
    * spark 停止后 程序停止前 调用
    */
  def beforeStop(ssc: StreamingContext): Unit = {}

  /**
    * 添加一个sparkListeners
    * 如使用此函数添加,则必须在 handle 之前调用此函数
    * @param listener
    * @deprecated 不建议使用此方法在代码中添加,如需添加请直接在配置文件中配置
    */
  @deprecated
  def addSparkListeners(listener: String): Unit = sparkListeners += listener

  /**
    * 处理函数
    *
    * @param ssc
    */
  def handle(ssc: StreamingContext): Unit

  private var heartbeat: Heartbeat = _

  /**
    * 心跳检测程序
    *
    * @param ssc
    */
  def heartbeat(ssc: StreamingContext): Unit = {
    heartbeat = new Heartbeat(ssc)
    heartbeat.start()
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

    init(sparkConf)

    val extraListeners = sparkListeners.mkString(",") + "," + sparkConf.get("spark.extraListeners", "")
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

    if (heartbeat != null) {
      heartbeat.stop()
    }
    beforeStop(context)
  }
}
