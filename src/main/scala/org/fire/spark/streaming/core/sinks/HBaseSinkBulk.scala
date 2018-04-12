package org.fire.spark.streaming.core.sinks

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

/**
  * 原本打算提供一个org.apache.hbase.spark.HBaseContext实现的bulkPut的功能
  * 但由于hbase-spark 目前不支持2.1.0以上的spark的版本
  * 此sink暂不可用
  * Created by cloud on 18/4/11.
  */
@deprecated
class HBaseSinkBulk[T: ClassTag](@transient override val sc: SparkContext,
                                 initParams: Map[String, String] = Map.empty[String, String]
                                ) extends Sink[T]{
  override val paramPrefix: String = "spark.sink.hbase."

  private lazy val prop = {
    val p = new Properties()
    p.putAll(param.map { case (k,v) => s"hbase.$k"->v} ++ initParams)
    p
  }

  /** 输出
    *
    * @param rdd
    * @param time
    */
  override def output(rdd: RDD[T], time: Time = Time(System.currentTimeMillis())): Unit = {

  }


}
