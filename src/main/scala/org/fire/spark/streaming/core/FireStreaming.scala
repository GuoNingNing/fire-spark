package org.fire.spark.streaming.core

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.fire.spark.streaming.core.kit.Utils

import scala.collection.mutable.ArrayBuffer

/**
  * Created by guoning on 2017/4/26.
  *
  * Spark Streaming 入口封装
  *
  */
trait FireStreaming {


  protected final def args: Array[String] = _args

  private final var _args: Array[String] = _

  // 添加debug 参数，便于统一控制
  private final var _debug: Boolean = false

  def isDebug: Boolean = _debug

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
    * StreamingContext 运行之后执行
    */
  def afterStarted(ssc: StreamingContext): Unit = {}

  /**
    * StreamingContext 停止后 程序停止前 执行
    */
  def beforeStop(ssc: StreamingContext): Unit = {}

  /**
    * 添加sparkListener
    * 如使用此函数添加,则必须在 handle 函数之前调用此函数
    * @param listener sparkListener的类名称,例:"org.apache.spark.TestListener"
    * @deprecated 建议直接在配置文件中添加
    */
  @deprecated
  def addSparkListeners(listener : String): Unit = sparkListeners += listener

  /**
    * 处理函数
    *
    * @param ssc
    */
  def handle(ssc: StreamingContext): Unit


  /**
    * 创建 Context
    *
    * @return
    */
  def creatingContext(): StreamingContext = {

    val sparkConf = new SparkConf()

    _debug = sparkConf.getBoolean("spark.fire.spark.debug", false)

    // 约定传入此参数,则表示本地 Debug
    if (sparkConf.contains("spark.properties.file")) {
      sparkConf.setAll(Utils.getPropertiesFromFile(sparkConf.get("spark.properties.file")))
      sparkConf.setAppName("LocalDebug").setMaster("local[*]")
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10")
    }

    init(sparkConf)

    val extraListeners = sparkListeners.mkString(",") + "," + sparkConf.get("spark.extraListeners","")
    if (extraListeners != "") sparkConf.set("spark.extraListeners",extraListeners)


    //    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    // 时间间隔
    val slide = sparkConf.get("spark.batch.duration").toInt
    //    val sc = sparkSession.sparkContext
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
        |   --monitor <是否开启监控  true|false>
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
        case ("--monitor") :: value :: tail =>
          monitor = value.toBoolean
          argv = tail
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
    context.awaitTermination()
    beforeStop(context)
  }
}
