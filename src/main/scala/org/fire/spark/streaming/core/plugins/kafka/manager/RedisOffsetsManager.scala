package org.fire.spark.streaming.core.plugins.kafka.manager


import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.fire.spark.streaming.core.plugins.redis.RedisConnectionPool
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._

/**
  * Created by guoning on 2017/10/19.
  *
  * Offset 存储到Redis
  */
private[kafka] class RedisOffsetsManager(val sparkConf: SparkConf) extends OffsetsManager {


  val jedis: Jedis = RedisConnectionPool.connect(storeParams)

  override def getOffsets(groupId: String, topics: Set[String]): Map[TopicPartition, Long] = {

    val offsets = topics.flatMap(
      topic => {
        jedis.hgetAll(generateKey(groupId, topic)).map {
          case (partition, offset) => new TopicPartition(topic, partition.toInt) -> offset.toLong
        }
      })

    logger.info(s"getOffsets [$groupId,${offsets.mkString(",")}] ")

    offsets.toMap
  }


  override def updateOffsets(groupId: String, offsetInfos: Map[TopicPartition, Long]): Unit = {

    for ((tp, offset) <- offsetInfos) {
      jedis.hset(generateKey(groupId, tp.topic), tp.partition().toString, offset.toString)
    }
    logger.info(s"updateOffsets [ $groupId,${offsetInfos.mkString(",")} ]")
  }

  override def delOffsets(groupId: String, topics: Set[String]): Unit = {
    for (topic <- topics) {
      jedis.del(generateKey(groupId, topic))
    }
    logger.info(s"delOffsets [ $groupId,${topics.mkString(",")} ]")
  }
}
