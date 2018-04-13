package org.fire.spark.streaming.core

import org.apache.spark.{SparkConf, SparkContext}
import org.fire.spark.streaming.core.kit.Utils

import scala.collection.mutable.ArrayBuffer

/**
  * Created by cloud on 18/4/12.
  */
trait FireSpark {

  protected final def args: Array[String] = _args

  private final var _args: Array[String] = _

  private val sparkListeners = new ArrayBuffer[String]()

  /**
    * 初始化，函数，可以设置 sparkConf
    *
    * @param sparkConf
    */
  def init(sparkConf: SparkConf): Unit = {}

  /**
    * spark 启动后 调用
    */
  def afterStarted(sc: SparkContext): Unit = {}

  /**
    * spark 停止后 程序结束前 调用
    */
  def beforeStop(sc: SparkContext): Unit = {}

  /**
    * 添加sparkListener
    * 如使用此函数添加,则必须在 handle 函数之前调用此函数
    * @param listener
    * @deprecated 建议直接在配置文件中添加
    */
  @deprecated
  def addSparkListeners(listener : String): Unit = sparkListeners += listener

  /**
    * 处理函数
    *
    * @param sc
    */
  def handle(sc: SparkContext): Unit

  def creatingContext(): SparkContext = {
    val sparkConf = new SparkConf()

    // 约定传入此参数,则表示本地 Debug
    if (sparkConf.contains("spark.properties.file")) {
      sparkConf.setAll(Utils.getPropertiesFromFile(sparkConf.get("spark.properties.file")))
      sparkConf.setAppName("LocalDebug").setMaster("local[*]")
      sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "10")
    }

    init(sparkConf)

    val extraListeners = sparkListeners.mkString(",") + "," + sparkConf.get("spark.extraListeners","")
    if (extraListeners != "") sparkConf.set("spark.extraListeners",extraListeners)

    val sc = new SparkContext(sparkConf)
    handle(sc)
    sc
  }

  def main(args: Array[String]): Unit = {

    this._args = args

    val context = creatingContext()
    afterStarted(context)
    context.stop()
    beforeStop(context)
  }

}
