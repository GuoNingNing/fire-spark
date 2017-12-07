package org.fire.spark.streaming.core.sources

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{Logging, SparkConf}

/**
  * Created by guoning on 16/8/2.
  *
  * 源
  */
trait Source[T] extends Serializable with Logging {
  @transient
  val ssc: StreamingContext
  @transient
  lazy val sparkConf: SparkConf = ssc.sparkContext.getConf

  /**
    * 获取DStream 流
    *
    * @return
    */
  def getDStream: DStream[T]
}
