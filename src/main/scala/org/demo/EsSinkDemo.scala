package org.demo

import java.util.UUID

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.fire.spark.streaming.core.FireStreaming
import org.fire.spark.streaming.core.sinks.EsSink
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.fire.spark.streaming.core.sources.KafkaDirectSource
import org.json4s.jackson.JsonMethods._

/**
  * Created by guoning on 2017/12/22.
  *
  */
object EsSinkDemo extends FireStreaming {

  implicit lazy val formats = org.json4s.DefaultFormats


  /**
    * 处理函数
    *
    * @param ssc
    */
  override def handle(ssc: StreamingContext): Unit = {

    val source = new KafkaDirectSource[String, String, StringDecoder, StringDecoder](ssc)

    val logs = source.getDStream[String](_.message())

    val jsons = logs.map(x => {
      val json = parse(x)

      val cv = (json \ "cv").extract[String]

      s"""{"cv":"$cv","name":"${UUID.randomUUID()}"}"""

    })

    jsons.print()

    jsons.foreachRDD((rdd, time) => {
      val esSink = new EsSink[String](rdd.sparkContext)
      esSink.output(rdd)
      source.updateZKOffsets(time.milliseconds)
    })

  }
}
