package org.fire.spark.streaming.core.plugins.kafka.manager

/**
  * Created by guoning on 16/6/22.
  *
  * 封装 Kafka
  */

import java.lang.reflect.Constructor
import java.{util => ju}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.fire.spark.streaming.core.Logging
import org.fire.spark.streaming.core.kit.Utils

import scala.reflect.ClassTag

private[kafka] class KafkaManager(val sparkConf: SparkConf) extends Logging with Serializable{


  // 自定义
  private lazy val offsetsManager = {
    sparkConf.get("spark.source.kafka.offset.store.class", "none").trim match {
      case "none" =>
        sparkConf.get("spark.source.kafka.offset.store.type", "none").trim.toLowerCase match {
          case "redis" => new RedisOffsetsManager(sparkConf)
          case "hbase" => new HbaseOffsetsManager(sparkConf)
          case "kafka" => new DefaultOffsetsManager(sparkConf)
          case "none" => new DefaultOffsetsManager(sparkConf)
        }
      case clazz =>

        logInfo(s"Custom offset management class $clazz")
        val constructors = {
          val offsetsManagerClass = Utils.classForName(clazz)
          offsetsManagerClass
            .getConstructors
            .asInstanceOf[Array[Constructor[_ <: SparkConf]]]
        }
        val constructorTakingSparkConf = constructors.find { c =>
          c.getParameterTypes.sameElements(Array(classOf[SparkConf]))
        }
        constructorTakingSparkConf.get.newInstance(sparkConf).asInstanceOf[OffsetsManager]
    }
  }


  def offsetManagerType = offsetsManager.storeType


  /**
    * 从Kafka创建一个 InputDStream[ConsumerRecord[K, V]]
    *
    * @param ssc
    * @param kafkaParams
    * @param topics
    * @tparam K
    * @tparam V
    * @return
    */
  def createDirectStream[K: ClassTag, V: ClassTag](ssc: StreamingContext,
                                                   kafkaParams: Map[String, Object],
                                                   topics: Set[String]
                                                  ): InputDStream[ConsumerRecord[K, V]] = {

    var consumerOffsets = Map.empty[TopicPartition, Long]

    kafkaParams.get("group.id") match {
      case Some(groupId) =>

        logInfo(s"createDirectStream witch group.id $groupId topics ${topics.mkString(",")}")

        consumerOffsets = offsetsManager.getOffsets(groupId.toString, topics)

      case _ =>
        logInfo(s"createDirectStream witchout group.id topics ${topics.mkString(",")}")
    }

    if (consumerOffsets.nonEmpty) {
      logInfo(s"read topics ==[$topics]== from offsets ==[$consumerOffsets]==")
      KafkaUtils.createDirectStream[K, V](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[K, V](consumerOffsets.keys, kafkaParams, consumerOffsets)
      )
    } else {
      // 匿名消费，生成临时消费组
      val groupId = kafkaParams.getOrElse("group.id", s"anonymity-${System.currentTimeMillis()}")
      logInfo(s"read topics ==[$topics]== by  group id ${groupId}==")

      KafkaUtils.createDirectStream[K, V](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[K, V](topics,  kafkaParams ++ Map("group.id" -> groupId))
      )
    }

  }

  /**
    *
    * @param sc
    * @param kafkaParams
    * @param offsetRanges
    * @param locationStrategy
    * @tparam K
    * @tparam V
    * @return
    */
  def createRDD[K: ClassTag, V: ClassTag](sc: SparkContext,
                                          kafkaParams: ju.Map[String, Object],
                                          offsetRanges: Array[OffsetRange],
                                          locationStrategy: LocationStrategy): RDD[ConsumerRecord[K, V]] = {
    KafkaUtils.createRDD(sc, kafkaParams, offsetRanges, locationStrategy)
  }


  /**
    * 更新 Offsets
    *
    * @param groupId
    * @param offsets
    */
  def updateOffsets(groupId: String, offsets: Array[OffsetRange]): Unit = {
    val offsetInfos = offsets.map(x => new TopicPartition(x.topic, x.partition) -> x.untilOffset).toMap
    offsetsManager.updateOffsets(groupId, offsetInfos)
  }

}

/**
  * Offset 管理
  */
trait OffsetsManager extends Logging with Serializable{

  val sparkConf: SparkConf

  lazy val storeParams: Map[String, String] = sparkConf
    .getAllWithPrefix(s"spark.source.kafka.offset.store.")
    .toMap

  lazy val storeType: String = storeParams.getOrElse("type", "none")

  /**
    * 获取存储的Offset
    *
    * @param groupId
    * @param topics
    * @return
    */
  def getOffsets(groupId: String, topics: Set[String]): Map[TopicPartition, Long]

  /**
    * 更新 Offsets
    *
    * @param groupId
    * @param offsetInfos
    */
  def updateOffsets(groupId: String, offsetInfos: Map[TopicPartition, Long]): Unit

  /**
    * 删除 Offsets
    *
    * @param groupId
    * @param topics
    */
  def delOffsets(groupId: String, topics: Set[String]): Unit = {}

  /**
    * 生成Key
    *
    * @param groupId
    * @param topic
    * @return
    */
  def generateKey(groupId: String, topic: String): String = s"$groupId#$topic"

}