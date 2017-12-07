package org.fire.spark.streaming.core.sinks

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.Map
import scala.language.postfixOps

/**
  * Created by guoning on 16/8/19.
  *
  * 输出ES
  */
class EsSink[T](override val ssc: StreamingContext,
                index: String, esType: String,
                initParams: Map[String, String] = Map.empty[String, String])
  extends Sink[T] {

  private lazy val esParams: Map[String, String] =
    sparkConf.getAll.filter(_._1.startsWith("spark.sink.es."))
      .map {
        case (k, v) => k.substring(11) -> v
      } toMap

  /**
    * 输出
    *
    */
  def output(rdd: RDD[T], time: Time = Time(System.currentTimeMillis())): Unit = {
    EsSpark.saveToEs(rdd, s"$index/$esType", esParams ++ initParams)
  }
}
