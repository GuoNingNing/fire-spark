package org.fire.spark.streaming.core.plugins.kafka

import java.util.Properties

import kafka.producer.{Producer, ProducerConfig}

import scala.collection.mutable

/**
  * a cache for producers
  */
object ProducerCache {

  private val producers = new mutable.HashMap[Properties, Any]()

  def getProducer[K, V](config: Properties): Producer[K, V] = {
    producers.getOrElse(config, {
      val producer = new Producer[K, V](new ProducerConfig(config))
      producers(config) = producer
      producer
    }).asInstanceOf[Producer[K, V]]
  }
}
