package org.fire.spark.streaming.core.sinks

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}

class DemoSink[T <: Product2[String, String]](override val sc: SparkContext) extends Sink[T] {
  /**
    * 输出
    *
    * @param rdd
    * @param time
    */
  override def output(rdd: RDD[T], time: Time): Unit = {


  }

  override val paramPrefix = ""
}
