package org.fire.spark.streaming.core.plugins.kafka.writer

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.fire.spark.streaming.core.plugins.kafka.ProducerCache

import scala.annotation.meta.param
import scala.reflect.ClassTag

/**
  * Created by guoning on 16/9/8.
  *
  * A simple Kafka producers
  */
class IterKafkaWrite[T: ClassTag](@(transient@param) msg: Iterator[T]) extends KafkaWriter[T] {
  /**
    *
    * @param producerConfig The configuration that can be used to connect to Kafka
    * @param serializerFunc The function to convert the data from the stream into Kafka
    *                       [[ProducerRecord]]s.
    * @tparam K The type of the key
    * @tparam V The type of the value
    *
    */
  override def writeToKafka[K, V](producerConfig: Properties, serializerFunc: (T) => ProducerRecord[K, V]): Unit = {
    val producer: KafkaProducer[K, V] = ProducerCache.getProducer(producerConfig)
    msg.map(serializerFunc).foreach(producer.send)
  }
}
