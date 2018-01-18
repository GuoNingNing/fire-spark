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


  //private lazy val jedis: Jedis = RedisConnectionPool.connect(storeParams)

  private def getJedis = RedisConnectionPool.connect(storeParams)

  override def getOffsets(groupId: String, topics: Set[String]): Map[TopicPartition, Long] = {
    val jedis = getJedis
    val offsets = topics.flatMap(
      topic => {
        jedis.hgetAll(generateKey(groupId, topic)).map {
          case (partition, offset) => new TopicPartition(topic, partition.toInt) -> offset.toLong
        }
      })
    jedis.close()
    logInfo(s"getOffsets [$groupId,${offsets.mkString(",")}] ")

    offsets.toMap
  }


  override def updateOffsets(groupId: String, offsetInfos: Map[TopicPartition, Long]): Unit = {
    val jedis = getJedis
    for ((tp, offset) <- offsetInfos) {
      jedis.hset(generateKey(groupId, tp.topic), tp.partition().toString, offset.toString)
    }
    jedis.close()
    logInfo(s"updateOffsets [ $groupId,${offsetInfos.mkString(",")} ]")
  }

  override def delOffsets(groupId: String, topics: Set[String]): Unit = {
    val jedis = getJedis
    for (topic <- topics) {
      jedis.del(generateKey(groupId, topic))
    }
    jedis.close()
    logInfo(s"delOffsets [ $groupId,${topics.mkString(",")} ]")
  }
}
