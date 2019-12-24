package org.fire.spark.streaming.core

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{JobInfoReportListener, Milliseconds, Seconds, StreamingContext}
import org.fire.spark.streaming.core.kit.Utils

import scala.annotation.meta.getter

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


    // checkpoint目录
    private var checkpointPath: String = ""

    // 从checkpoint 中恢复失败，则重新创建
    private var createOnError: Boolean = true


    @(transient@getter)
    var sparkSession: SparkSession = _

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
      * 自定义sparkStreamingListener 加载
      *
      * @param ssc
      * @param listener
      * @return
      */
    def addSparkStreamingListeners(ssc: StreamingContext, listener: String): StreamingContext = {

        listener.split(",")
                .map(_.trim)
                .foreach {
                    case "JobInfoReportListener" | "org.apache.spark.streaming.JobInfoReportListener" => {
                        ssc.addStreamingListener(new JobInfoReportListener(ssc))
                    }
                    case "CongestionMonitorListener" | "org.apache.spark.streaming.CongestionMonitorListener" => {
                        ssc.addStreamingListener(new JobInfoReportListener(ssc))
                    }
                    case _ => {
                        println("invalid listener")
                    }
                }
        ssc
    }

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

        sparkConf.set("spark.user.args", args.mkString("|"))
        _debug = sparkConf.getBoolean("spark.fire.spark.debug", false)

        // 约定传入此参数,则表示本地 Debug
        if (sparkConf.contains("spark.properties.file")) {
            sparkConf.setAll(Utils.getPropertiesFromFile(sparkConf.get("spark.properties.file")))
            sparkConf.setAppName("LocalDebug").setMaster("local[*]")
            sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10")
        }

        init(sparkConf)


        sparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

        // 时间间隔
        val slide = sparkConf.get("spark.batch.duration").toDouble

        val batchTime = if (slide >= 1.0) {
            Seconds(slide.toInt)
        } else {
            Milliseconds((slide * 1000).toInt)
        }
        val ssc = new StreamingContext(sparkSession.sparkContext, batchTime)

        handle(addSparkStreamingListeners(ssc, sparkConf.get("spark.user.extraListeners", "")))
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
        context.awaitTermination()
        beforeStop(context)
    }
}
