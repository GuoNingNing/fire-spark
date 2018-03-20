package org.fire.spark.streaming.core

import org.apache.spark.{SparkConf, SparkContext}
import org.fire.spark.streaming.core.kit.Utils

trait FireSpark {

  /**
    * 初始化，函数，可以设置 sparkConf
    *
    * @param sparkConf
    */
  def init(sparkConf: SparkConf): Unit = {}

  /**
    * 处理函数
    *
    * @param ssc
    */
  def handle(ssc: SparkContext): Unit

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

    val context = creatingContext()

    context.stop()
  }

}
