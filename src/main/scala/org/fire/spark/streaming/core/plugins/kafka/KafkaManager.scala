package org.apache.spark.streaming.kafka

/**
  * Created by guoning on 16/6/22.
  *
  * 封装 Kafka
  */

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.LeaderOffset
import org.apache.spark.{Logging, SparkException}

import scala.reflect.ClassTag

class KafkaManager(val kafkaParams: Map[String, String]) extends Logging with Serializable {

  private val kc = new KafkaCluster(kafkaParams)


  /**
    * 创建数据流
    *
    * @param ssc
    * @param kafkaParams
    * @param topics
    * @tparam K
    * @tparam V
    * @tparam KD
    * @tparam VD
    * @return
    */
  def createDirectStream[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag,
                         VD <: Decoder[V] : ClassTag, R: ClassTag](ssc: StreamingContext,
                                            kafkaParams: Map[String, String],
                                            topics: Set[String],
                                            messageHandler: MessageAndMetadata[K, V] => R
                                           ): InputDStream[R] = {

    kafkaParams.get("group.id") match {
      case Some(groupId) =>
        logInfo(s"createDirectStream witch group.id $groupId topics ${topics.mkString(",")}")

        // 在zookeeper上读取offsets前先根据实际情况更新offsets
        setOrUpdateOffsets(topics, groupId)

        //从zookeeper上读取offset开始消费message
        val partitionsE = kc.getPartitions(topics)

        if (partitionsE.isLeft)
          throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")

        val partitions = partitionsE.right.get
        val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)

        if (consumerOffsetsE.isLeft)
          throw new SparkException(s"get kafka consumer offsets failed: ${consumerOffsetsE.left.get}")

        val consumerOffsets = consumerOffsetsE.right.get

        logInfo(s"read topics ==[$topics]== from offsets ==[$consumerOffsets]==")

        KafkaUtils.createDirectStream[K, V, KD, VD, R](ssc, kafkaParams, consumerOffsets, messageHandler)
      case None =>
        logInfo(s"createDirectStream witchout group.id topics ${topics.mkString(",")}")
        val consumerOffsets = getFromOffsets(kc, kafkaParams, topics)

        logInfo(s"read topics ==[$topics]== from offsets ==[$consumerOffsets]==")
        KafkaUtils.createDirectStream[K, V, KD, VD, R](ssc, kafkaParams, consumerOffsets, messageHandler)
    }
  }

  def getFromOffsets(kc: KafkaCluster,
                     kafkaParams: Map[String, String],
                     topics: Set[String]
                    ): Map[TopicAndPartition, Long] = {
    val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
    val result = for {
      topicPartitions <- kc.getPartitions(topics).right
      leaderOffsets <- (if (reset == Some("smallest")) {
        kc.getEarliestLeaderOffsets(topicPartitions)
      } else {
        kc.getLatestLeaderOffsets(topicPartitions)
      }).right
    } yield {
      leaderOffsets.map { case (tp, lo) =>
        (tp, lo.offset)
      }
    }
    KafkaCluster.checkErrors(result)
  }

  /**
    * 创建数据流前，根据实际消费情况更新消费offsets
    *
    * @param topics  消费Topic
    * @param groupId 分组
    */
  private def setOrUpdateOffsets(topics: Set[String], groupId: String): Unit = {
    topics.foreach(topic => {
      var hasConsumed = true
      val partitionsE = kc.getPartitions(Set(topic))

      if (partitionsE.isLeft)
        throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")

      val partitions = partitionsE.right.get
      val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)
      if (consumerOffsetsE.isLeft) hasConsumed = false

      if (hasConsumed) {

        /**
          * 如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，
          * 说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
          * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
          * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
          * 这时把consumerOffsets更新为earliestLeaderOffsets
          */
        val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
        if (earliestLeaderOffsetsE.isLeft)
          throw new SparkException(s"get earliest leader offsets failed: ${earliestLeaderOffsetsE.left.get}")
        val earliestLeaderOffsets = earliestLeaderOffsetsE.right.get
        val consumerOffsets = consumerOffsetsE.right.get

        // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
        var offsets: Map[TopicAndPartition, Long] = Map()
        consumerOffsets.foreach({ case (tp, n) =>
          val earliestLeaderOffset = earliestLeaderOffsets(tp).offset
          if (n < earliestLeaderOffset) {
            logInfo("consumer group:" + groupId + ",topic:" + tp.topic + ",partition:" + tp.partition +
              " offsets已经过时，更新为" + earliestLeaderOffset)
            offsets += (tp -> earliestLeaderOffset)
          }
        })
        if (offsets.nonEmpty) {
          kc.setConsumerOffsets(groupId, offsets)
        }
      } else {
        // 没有消费过
        val reset = kafkaParams.get("auto.offset.reset").map(_.toLowerCase)
        var leaderOffsets: Map[TopicAndPartition, LeaderOffset] = null
        if (reset == Some("smallest")) {
          val leaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft)
            throw new SparkException(s"get earliest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        } else {
          val leaderOffsetsE = kc.getLatestLeaderOffsets(partitions)
          if (leaderOffsetsE.isLeft)
            throw new SparkException(s"get latest leader offsets failed: ${leaderOffsetsE.left.get}")
          leaderOffsets = leaderOffsetsE.right.get
        }
        val offsets = leaderOffsets.map {
          case (tp, offset) => (tp, offset.offset)
        }
        kc.setConsumerOffsets(groupId, offsets)
      }
    })
  }


  /**
    * 获取指定topic的分区信息
    *
    * @param topics
    */
  def getPartitions(topics: Set[String]): Set[TopicAndPartition] = {
    val partitionsE = kc.getPartitions(topics)
    if (partitionsE.isLeft)
      throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")
    partitionsE.right.get
  }

  /**
    * 获取ZK 上对应  groupid 、 topic 的 offset
    *
    * @param groupId
    * @param topic
    * @return
    */
  def getZKOffsets(groupId: String, topic: String): List[(TopicAndPartition, Long)] = {
    val partitionsE = kc.getPartitions(Set(topic))

    if (partitionsE.isLeft) throw new SparkException(s"get kafka partition failed: ${partitionsE.left.get}")

    val partitions = partitionsE.right.get
    val consumerOffsetsE = kc.getConsumerOffsets(groupId, partitions)


    val earliestLeaderOffsetsE = kc.getEarliestLeaderOffsets(partitions)

    if (earliestLeaderOffsetsE.isLeft)
      throw new SparkException(s"get earliest leader offsets failed: ${earliestLeaderOffsetsE.left.get}")

    consumerOffsetsE.right.get.toList

  }

  /**
    * 更新zookeeper上的消费offsets
    *
    * @param rdd
    */
  def updateZKOffsets(rdd: RDD[(String, String)]): Unit = {
    val groupId = kafkaParams.getOrElse("group.id", "offset_test")
    val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    updateZKOffsets(groupId, offsetRanges)
  }

  /**
    * 更新offset
    *
    * @param groupId
    * @param offsetRanges
    */
  def updateZKOffsets(groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    val offsetMap = offsetRanges.map(offset =>
      TopicAndPartition(offset.topic, offset.partition) -> offset.untilOffset
    ).toMap
    updateZKOffsets(groupId, offsetMap)
  }


  /**
    * 更新 offset 为最早记录
    *
    * @param groupId
    * @param topicAndPartitions
    */
  def updateZKOffsets2Earliest(groupId: String, topicAndPartitions: Set[TopicAndPartition]): Unit = {
    val earlist = kc.getEarliestLeaderOffsets(topicAndPartitions)

    val leaderOffsets = earlist.right.get
    val offsetMap: Map[TopicAndPartition, Long] =
      leaderOffsets.flatMap {
        case (tp: TopicAndPartition, offset) => Some(tp -> offset.offset)
        case _ => None
      }
    updateZKOffsets(groupId, offsetMap)
  }


  /**
    * 更新 offset 为最新记录
    *
    * @param groupId
    * @param topicAndPartitions
    */
  def updateZKOffsets2Latest(groupId: String, topicAndPartitions: Set[TopicAndPartition]): Unit = {
    val earlist = kc.getLatestLeaderOffsets(topicAndPartitions)

    val leaderOffsets = earlist.right.get
    val offsetMap: Map[TopicAndPartition, Long] = leaderOffsets.flatMap {
      case (tp: TopicAndPartition, offset) => Some(tp -> offset.offset)
      case _ => None
    }
    updateZKOffsets(groupId, offsetMap)
  }


  /**
    * 更新 Offset
    *
    * @param groupId
    * @param offsetMap
    */
  def updateZKOffsets(groupId: String, offsetMap: Map[TopicAndPartition, Long]): Unit = {
    logInfo(s"update offset group.id : $groupId offsetMap $offsetMap")
    val o = kc.setConsumerOffsets(groupId, offsetMap)
    if (o.isLeft) {
      logInfo(s"Error updating the offset to Kafka cluster: ${o.left.get}")
    }
  }

  /**
    * 更新offset
    *
    * @param groupId
    * @param offsets
    */
  def updateZKOffsets(groupId: String, offsets: Set[Map[TopicAndPartition, Long]]): Unit = {
    for (offset <- offsets) {
      updateZKOffsets(groupId, offset)
    }
  }

}

