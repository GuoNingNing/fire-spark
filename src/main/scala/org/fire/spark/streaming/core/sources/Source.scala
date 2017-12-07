package org.fire.spark.streaming.core.sources

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{Logging, SparkConf}

import scala.reflect.ClassTag

/**
  * Created by guoning on 16/8/2.
  *
  * 源
  */
trait Source extends Serializable with Logging {
  @transient
  val ssc: StreamingContext
  @transient
  lazy val sparkConf: SparkConf = ssc.sparkContext.getConf

  type SourceType

  /**
    * 获取DStream 流
    *
    * @return
    */
  def getDStream[R: ClassTag](f: SourceType => R): DStream[R]
}
