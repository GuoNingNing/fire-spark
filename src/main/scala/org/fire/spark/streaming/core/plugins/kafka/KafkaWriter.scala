package org.fire.spark.streaming.core.plugins.kafka

import java.util.Properties

import kafka.producer.KeyedMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
  * Import this object in this form:
  * {{{
  *   import org.cloudera.spark.streaming.kafka.KafkaWriter._
  * }}}
  *
  * Once this is done, the `writeToKafka` can be called on the [[DStream]] object in this form:
  * {{{
  *   dstream.writeToKafka(producerConfig, f)
  * }}}
  */
object KafkaWriter {
    /**
      * This implicit method allows the user to call dstream.writeToKafka(..)
      *
      * @param dstream - DStream to write to Kafka
      * @tparam T - The type of the DStream
      * @tparam K - The type of the key to serialize to
      * @tparam V - The type of the value to serialize to
      * @return
      */
    implicit def createKafkaOutputWriter[T: ClassTag, K, V](dstream: DStream[T]): KafkaWriter[T] = {
        new DStreamKafkaWriter[T](dstream)
    }

    implicit def createKafkaOutputWriter[T: ClassTag, K, V](rdd: RDD[T]): KafkaWriter[T] = {
        new RDDKafkaWriter[T](rdd)
    }

    implicit def createKafkaOutputWriter[T: ClassTag, K, V](msg: Iterator[T]): KafkaWriter[T] = {
        new IterKafkaWrite[T](msg)
    }

    implicit def createKafkaOutputWriter[T: ClassTag, K, V](msg: T): KafkaWriter[T] = {
        new SimpleKafkaWrite[T](msg)
    }
}

/**
  *
  * This class can be used to write data to Kafka from Spark Streaming. To write data to Kafka
  * simply `import org.cloudera.spark.streaming.kafka.KafkaWriter._` in your application and call
  * `dstream.writeToKafka(producerConf, func)`
  *
  * Here is an example:
  * {{{
  * // Adding this line allows the user to call dstream.writeDStreamToKafka(..)
  * import org.apache.spark.streaming.kafka.KafkaWriter._
  *
  * class ExampleWriter {
  *   val instream = ssc.queueStream(toBe)
  *   val producerConf = new Properties()
  *   producerConf.put("serializer.class", "kafka.serializer.DefaultEncoder")
  *   producerConf.put("key.serializer.class", "kafka.serializer.StringEncoder")
  *   producerConf.put("metadata.broker.list", "kafka.example.com:5545")
  *   producerConf.put("request.required.acks", "1")
  *   instream.writeToKafka(producerConf,
  *   (x: String) => new KeyedMessage[String,String]("default", null, x))
  *   ssc.start()
  * }
  *
  * }}}
  *
  */
abstract class KafkaWriter[T: ClassTag]() {

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
    def writeToKafka[K, V](producerConfig: Properties, serializerFunc: T => KeyedMessage[K, V]): Unit
}
