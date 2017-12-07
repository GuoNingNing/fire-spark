package org.fire.spark.streaming.core.plugins.kafka

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer}

import scala.reflect.ClassTag

/**
  * Created by guoning on 16/9/8.
  *
  * A simple Kafka producers
  */
class IterKafkaWrite[T: ClassTag](@transient msg: Iterator[T]) extends KafkaWriter[T] {
  /**
    * To write data from a Iterator to Kafka, call this function after creating the Iterator. Once
    * the Iterator is passed into this function, all data coming from the Iterator is written out to
    * Kafka. The properties instance takes the configuration required to connect to the Kafka
    * brokers in the standard Kafka format. The serializerFunc is a function that converts each
    * element of the RDD to a Kafka [[KeyedMessage]]. This closure should be serializable - so it
    * should use only instances of Serializables.
    *
    * @param producerConfig The configuration that can be used to connect to Kafka
    * @param serializerFunc The function to convert the data from the stream into Kafka
    *                       [[KeyedMessage]]s.
    * @tparam K The type of the key
    * @tparam V The type of the value
    *
    */
  override def writeToKafka[K, V](producerConfig: Properties, serializerFunc: (T) => KeyedMessage[K, V]): Unit = {
    val producer: Producer[K, V] = ProducerCache.getProducer(producerConfig)
    producer.send(msg.map(serializerFunc).toArray: _*)
  }
}
