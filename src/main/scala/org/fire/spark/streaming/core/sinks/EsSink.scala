package org.fire.spark.streaming.core.sinks

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.elasticsearch.spark.rdd.EsSpark

import scala.annotation.meta.param
import scala.collection.Map
import scala.language.postfixOps

/**
  * Created by guoning on 16/8/19.
  *
  * 输出ES
  */
class EsSink[T](override val sc: SparkContext, initParams: Map[String, String] = Map.empty[String, String])
  extends Sink[T] {

  private lazy val esParams: Map[String, String] = param ++ initParams

  //  logger.info("es params {}", esParams)

  lazy val index: String = esParams("es.index")
  lazy val esType: String = esParams("es.type")

  /**
    * 输出
    *
    */
  def output(rdd: RDD[T], time: Time = Time(System.currentTimeMillis())): Unit = {

    rdd match {
      case _: RDD[String] => EsSpark.saveJsonToEs(rdd, s"$index/$esType", esParams)
      case _ => EsSpark.saveToEs(rdd, s"$index/$esType", esParams)
    }


  }

  override val paramPrefix: String = "spark.sink.es."
}
