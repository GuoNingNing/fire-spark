package org.fire.spark.streaming.core

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}
import org.fire.spark.streaming.core.kit.Utils
import org.slf4j.LoggerFactory

trait FireSpark {

  lazy val logger = LoggerFactory.getLogger(getClass)

  protected final def args: Array[String] = _args

  private final var _args: Array[String] = _

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

    val sc = new SparkContext(sparkConf)
    handle(sc)
    sc
  }

  def main(args: Array[String]): Unit = {

    this._args = args

    val context = creatingContext()
    afterStarted(context)
    context.stop()
  }

}