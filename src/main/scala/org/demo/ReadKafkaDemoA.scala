package org.demo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.fire.spark.streaming.core.kit.Heartbeat
import org.fire.spark.streaming.core.{FireStreaming, Logging}
import org.fire.spark.streaming.core.plugins.kafka.KafkaDirectSource
import org.fire.spark.streaming.core.plugins.redis.RedisConnectionPool

import scala.reflect.ClassTag

object ReadKafkaDemoA extends FireStreaming with Logging {


  /**
    * 处理函数
    *
    * @param ssc
    */
  override def handle(ssc: StreamingContext): Unit = {

    val source = new KafkaDirectSource[String, String](ssc)

    val logs = source.getDStream[String](_.value())

    logs.print()

    logs.foreachRDD((rdd,time) => {
      rdd.foreach(println)

      source.updateOffsets(time.milliseconds)

    })


  }




}
