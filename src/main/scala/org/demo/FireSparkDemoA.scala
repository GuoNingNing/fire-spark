package org.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.fire.spark.streaming.core.{FireSpark, Logging}

/**
  * Created by cloud on 18/4/13.
  */
object FireSparkDemoA extends FireSpark with Logging {

  /** 初始化，函数，可以设置 sparkConf
    *
    * @param sparkConf
    */
  override def init(sparkConf: SparkConf): Unit = {
    logInfo("init sparkConf")
    addSparkListeners("org.apache.spark.TestListener")
  }

  /** 处理函数
    *
    * @param sc
    */
  override def handle(sc: SparkContext): Unit = {
    val rdd = sc.textFile("xxxx")
    rdd.flatMap(_.split(" ")).map(_ -> 1).reduceByKey(_ + _).foreach(println)
  }

}
