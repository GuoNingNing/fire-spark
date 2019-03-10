package org.fire.spark.streaming.core.kit

import kafka.common.TopicAndPartition
import org.fire.spark.streaming.core.plugins.kafka.KafkaManager

/**
  * Created by guoning on 16/8/4.
  *
  * Kafka 工具包
  *
  */
object KafkaKit {


  /**
    * 获取指定 groupId topic 的Offset信息
    *
    * @param brokers
    * @param groupId
    * @param topic
    * @return
    */
  def getZKOffsets(brokers: String, groupId: String, topic: String): List[(TopicAndPartition, Long)] = {
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "group.id" -> groupId)
    val km = new KafkaManager(kafkaParams)

    km.getZKOffsets(groupId, topic)
  }

  /**
    * 设置offsete 为 最早
    *
    * @param brokers
    * @param groupId
    * @param topic
    */
  def updateZKOffsets2Earliest(brokers: String, groupId: String, topic: String) {

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "group.id" -> groupId)
    val km = new KafkaManager(kafkaParams)

    km.updateZKOffsets2Earliest(groupId, km.getPartitions(Set(topic)))
  }


  /**
    * 设置 offset 为 最新
    *
    * @param brokers
    * @param groupId
    * @param topic
    */
  def updateZKOffsets2Latest(brokers: String, groupId: String, topic: String) {

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "group.id" -> groupId)
    val km = new KafkaManager(kafkaParams)

    km.updateZKOffsets2Latest(groupId, km.getPartitions(Set(topic)))
  }

  /**
    * 设置 offset 为指定值
    *
    * @param brokers
    * @param groupId
    * @param topic
    * @param offsets
    */
  def updateZKOffsets(brokers: String, groupId: String, topic: String, offsets: Set[Map[TopicAndPartition, Long]]) {

    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "group.id" -> groupId)
    val km = new KafkaManager(kafkaParams)

    km.updateZKOffsets(groupId, offsets)
  }


  /**
    * 获取指定topic 的分区信息
    *
    * @param brokers
    * @param topics
    * @return
    */
  def getPartitions(brokers: String, topics: Set[String]): Set[TopicAndPartition] = {
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
    val km = new KafkaManager(kafkaParams)
    km.getPartitions(topics)
  }

  /**
    * 同步Offset
    *
    * @param brokers
    * @param srcGroupId
    * @param tagGroupId
    * @param topic
    */
  def syncOffset(brokers: String, srcGroupId: String, tagGroupId: String, topic: String): Unit = {
    val zkOffset = KafkaKit.getZKOffsets(brokers: String, srcGroupId, topic)
      .map(x => Map(x._1 -> x._2)).toSet

    KafkaKit.updateZKOffsets(brokers: String, tagGroupId, topic, zkOffset
    )
  }

}
