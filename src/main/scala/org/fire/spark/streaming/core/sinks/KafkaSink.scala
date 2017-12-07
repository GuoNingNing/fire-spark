package org.fire.spark.streaming.core.sinks

import java.util.{Properties, UUID}

import kafka.producer.KeyedMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.fire.spark.streaming.core.plugins.kafka.KafkaWriter._

import scala.reflect.ClassTag

/**
  * Created by guoning on 16/8/2.
  *
  * 输出到kafka
  */
class KafkaSink[T: ClassTag](val ssc: StreamingContext)
  extends Sink[T] {

  private val producerConf = new Properties()
  producerConf.put("serializer.class", "kafka.serializer.DefaultEncoder")
  producerConf.put("key.serializer.class", "kafka.serializer.StringEncoder")
  producerConf.put("metadata.broker.list", sparkConf.get("spark.sink.kafka.metadata.broker.list"))

  private val outputTopic = sparkConf.get("spark.sink.kafka.topic")

  /**
    * 以字符串的形式输出到kafka
    *
    */
  override def output(rdd: RDD[T], time: Time = Time(System.currentTimeMillis())): Unit = {

    rdd.writeToKafka(producerConf, x => new KeyedMessage[String, Array[Byte]](outputTopic, UUID.randomUUID().toString, x.toString.getBytes()))
  }
}
