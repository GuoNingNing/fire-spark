package org.fire.spark.streaming.core.plugins.kafka

import java.util.Properties

import kafka.producer.KeyedMessage
import org.apache.spark.streaming.dstream.DStream

import scala.annotation.meta.param
import scala.reflect.ClassTag

class DStreamKafkaWriter[T: ClassTag](@(transient@param) dstream: DStream[T]) extends KafkaWriter[T] {

    /**
      * To write data from a DStream to Kafka, call this function after creating the DStream. Once
      * the DStream is passed into this function, all data coming from the DStream is written out to
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
    override def writeToKafka[K, V](producerConfig: Properties,
                                    serializerFunc: T => KeyedMessage[K, V]): Unit = {
        dstream.foreachRDD { rdd =>
            val rddWriter = new RDDKafkaWriter[T](rdd)
            rddWriter.writeToKafka(producerConfig, serializerFunc)
        }
    }
}
