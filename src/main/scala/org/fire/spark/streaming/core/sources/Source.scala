package org.fire.spark.streaming.core.sources

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

import scala.annotation.meta.getter
import scala.reflect.ClassTag

/**
  * Created by guoning on 16/8/2.
  *
  * 源
  */
trait Source extends Serializable {
  lazy val logger = LoggerFactory.getLogger(getClass)
  @(transient@getter)
  val ssc: StreamingContext
  @(transient@getter)
  lazy val sparkConf: SparkConf = ssc.sparkContext.getConf

  val paramPrefix: String

  type SourceType

  /**
    * 获取DStream 流
    *
    * @return
    */
  def getDStream[R: ClassTag](f: SourceType => R): DStream[R]
}
