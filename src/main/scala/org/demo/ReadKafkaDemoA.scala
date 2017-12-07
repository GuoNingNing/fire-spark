package org.demo

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.fire.spark.streaming.core.FireStreaming
import org.fire.spark.streaming.core.sources.KafkaDirectSource

object ReadKafkaDemoA extends FireStreaming {
  /**
    * 处理函数
    *
    * @param ssc
    */
  override def handle(ssc: StreamingContext): Unit = {

    val source = new KafkaDirectSource[String, String, StringDecoder, StringDecoder](ssc)

    val logs = source.getDStream[String](_.message())

    logs.foreachRDD((rdd, time) => {
      rdd.take(10).foreach(println)
      source.updateZKOffsets(time.milliseconds)
    })
  }
}
