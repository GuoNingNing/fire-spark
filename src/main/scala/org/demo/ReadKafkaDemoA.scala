package org.demo

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.fire.spark.streaming.core.FireStreaming
import org.fire.spark.streaming.core.sources.KafkaDirectSource

object ReadKafkaDemoA extends FireStreaming {

  /**
    * 初始化函数
    */
  override def init(sparkConf: SparkConf): Unit = {
    sparkConf.set("kafka.auto.offset","false")
    addSparkListeners("org.apache.spark.TestListener")
  }

  /**
    * 处理函数
    *
    * @param ssc
    */
  override def handle(ssc: StreamingContext): Unit = {

    val source = new KafkaDirectSource[String, String, StringDecoder, StringDecoder](ssc)

    val logs = source.getDStream(r => r.message())

    logs.foreachRDD((rdd, time) => {
      rdd.take(10).foreach(println)
      source.updateOffsets(time.milliseconds)
    })


  }
}
