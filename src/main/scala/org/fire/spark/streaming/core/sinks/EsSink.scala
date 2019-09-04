package org.fire.spark.streaming.core.sinks


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.Map
import scala.language.postfixOps

/**
  * Created by guoning on 16/8/19.
  *
  * 输出ES
  */
class EsSink[T](@transient override val sc: SparkContext,
                initParams: Map[String, String] = Map.empty[String, String])
    extends Sink[T] {

    override val paramPrefix: String = "spark.sink.es."


    lazy val esParam: Predef.Map[String, String] = param.map { case (k, v) => s"es.$k" -> v } ++ initParams

    /**
      * 输出
      *
      */
    def output(rdd: RDD[T], time: Time = Time(System.currentTimeMillis())): Unit = {
        rdd match {
            case rdd: RDD[String] => EsSpark.saveJsonToEs(rdd, esParam)
            case _ => EsSpark.saveToEs(rdd, esParam)
        }
    }
}
