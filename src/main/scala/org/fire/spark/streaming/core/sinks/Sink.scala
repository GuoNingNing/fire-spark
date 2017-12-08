package org.fire.spark.streaming.core.sinks

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Time}
import org.slf4j.LoggerFactory

import scala.annotation.meta.getter

/**
  * Created by guoning on 16/8/2.
  */
trait Sink[T] extends Serializable {

  lazy val logger = LoggerFactory.getLogger(getClass)

  @(transient @getter)
  val ssc: StreamingContext
  @(transient @getter)
  lazy val sparkConf = ssc.sparkContext.getConf

  /**
    * 输出
    *
    */
  def output(dStream: DStream[T]): Unit = {
    dStream.foreachRDD((rdd, time) => output(rdd, time))
  }

  /**
    * 输出
    *
    * @param rdd
    * @param time
    */
  def output(rdd: RDD[T], time: Time)
}
