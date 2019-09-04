package org.fire.spark.streaming.core.plugins.kafka.manager

import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf

/**
  * Created by guoning on 2017/10/20.
  *
  * 默认Offset管理，不做任何操作
  */
class DefaultOffsetsManager(val sparkConf: SparkConf) extends OffsetsManager {

    /**
      * 获取存储的Offset
      *
      * @param groupId
      * @param topics
      * @return
      */
    override def getOffsets(groupId: String, topics: Set[String]): Map[TopicPartition, Long] = {
        Map.empty[TopicPartition, Long]
    }

    /**
      * 更新 Offsets
      *
      * @param groupId
      * @param offsetInfos
      */
    override def updateOffsets(groupId: String, offsetInfos: Map[TopicPartition, Long]): Unit = {

    }
}
