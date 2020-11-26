package org.fire.spark.streaming.core.plugins.kafka.manager

/**
 * Created by guoning on 16/6/22.
 *
 * 封装 Kafka
 */

import java.lang.reflect.Constructor
import java.time.Duration
import java.{util => ju}

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndTimestamp}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ListOffsetRequest
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import org.fire.spark.streaming.core.Logging
import org.fire.spark.streaming.core.kit.Utils

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.Try

private[kafka] class KafkaManager(val sparkConf: SparkConf) extends Logging with Serializable {


  // 自定义
  private lazy val offsetsManager = {
    sparkConf.get("spark.source.kafka.offset.store.class", "none").trim match {
      case "none" =>

        sparkConf.get("spark.source.kafka.offset.store.type", "none").trim.toLowerCase match {
          case "redis" =>
            sparkConf.get("spark.source.kafka.offset.store.redis.cluster", "false") match {
              case "true" => new RedisClusterOffsetsManager(sparkConf)
              case _ => new RedisOffsetsManager(sparkConf)
            }

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


  def offsetManagerType: String = offsetsManager.storeType


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
      KafkaUtils.createDirectStream[K, V](ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[K, V](topics, kafkaParams)
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
trait OffsetsManager extends Logging with Serializable {

  val sparkConf: SparkConf

  lazy val storeParams: Map[String, String] = sparkConf
    .getAllWithPrefix(s"spark.source.kafka.offset.store.")
    .toMap

  lazy val storeType: String = storeParams.getOrElse("type", "none")

  lazy val kafkaParams = sparkConf
    .getAllWithPrefix(s"spark.source.kafka.consume.")
    .toMap

  lazy val reset = sparkConf.get("spark.source.kafka.consume.auto.offset.reset", "largest")

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


  //  def getLeaders(topics: Seq[String]): Map[(String, Int), Seq[TopicPartition]] = {
  //    val consumer = new SimpleConsumer(host, port, 100000, 64 * 1024, s"leaderLookup-${System.currentTimeMillis()}")
  //    val req = new TopicMetadataRequest(topics, 0)
  //    val resp = consumer.send(req)
  //
  //    val leaderAndTopicPartition = new mutable.HashMap[(String, Int), Seq[TopicPartition]]()
  //
  //    resp.topicsMetadata.foreach((metadata: TopicMetadata) => {
  //      val topic = metadata.topic
  //      metadata.partitionsMetadata.foreach((partition: PartitionMetadata) => {
  //        partition.leader match {
  //          case Some(endPoint) =>
  //            val hp = Tuple2(endPoint.host, endPoint.port)
  //            leaderAndTopicPartition.get(hp) match {
  //              case Some(taps) => leaderAndTopicPartition.put(hp, taps :+ new TopicPartition(topic, partition.partitionId))
  //              case None => leaderAndTopicPartition.put(hp, Seq(new TopicPartition(topic, partition.partitionId)))
  //            }
  //          case None => throw new SparkException(s"get topic[$topic] partition[${partition.partitionId}] leader failed")
  //        }
  //      })
  //    })
  //    Try(consumer.close())
  //    leaderAndTopicPartition.toMap
  //  }

  /**
   * 获取最旧的Offsets
   *
   * @param topics
   * @return
   */
  def getEarliestOffsets(topics: Seq[String]): Map[TopicPartition, Long] = getOffsets(topics, ListOffsetRequest.EARLIEST_TIMESTAMP)

  /**
   * 获取最新的Offset
   *
   * @param topics
   * @return
   */
  def getLatestOffsets(topics: Seq[String]): Map[TopicPartition, Long] = getOffsets(topics, ListOffsetRequest.LATEST_TIMESTAMP)

  /**
   * 获取指定时间的Offset
   * 想
   *
   * @param topics
   * @param time
   * @return
   */
  def getOffsets(topics: Seq[String], time: Long): Map[TopicPartition, Long] = {


    val offsetMap = new mutable.HashMap[TopicPartition, Long]()

    val configs = new ju.HashMap[String, Object]()

    kafkaParams.foreach { case (k, v) => configs.put(k, v) }

    val _consumer = new KafkaConsumer[String, String](configs)

    _consumer.subscribe(topics.asJava)

    val timestampsToSearch = new ju.HashMap[TopicPartition, java.lang.Long]()
    val topicPartitions = new ju.ArrayList[TopicPartition]()

    topics.flatMap(t => _consumer.partitionsFor(t).asScala).foreach(pInfo => {
      timestampsToSearch.put(new TopicPartition(pInfo.topic(), pInfo.partition()), time)
      topicPartitions.add(new TopicPartition(pInfo.topic(), pInfo.partition()))
    })

    time match {
      case ListOffsetRequest.EARLIEST_TIMESTAMP =>
        _consumer.beginningOffsets(topicPartitions).asScala.foreach {
          case (tap, offset) => offsetMap.put(new TopicPartition(tap.topic, tap.partition), offset)
        }
      case ListOffsetRequest.LATEST_TIMESTAMP =>
        _consumer.endOffsets(topicPartitions).asScala.foreach {
          case (tap, offset) => offsetMap.put(new TopicPartition(tap.topic, tap.partition), offset)
        }
      case other =>
        _consumer.offsetsForTimes(timestampsToSearch).asScala.foreach {
          case (tap, offset) => offsetMap.put(new TopicPartition(tap.topic, tap.partition), offset.offset())
        }
    }

    Try(_consumer.close())
    offsetMap.toMap


    //    val leaders = getLeaders(topics)


    //    leaders.foreach {
    //      case ((leaderHost, _port), taps) =>
    //        val consumer = new SimpleConsumer(leaderHost, _port, 100000, 64 * 1024, s"offsetLookup-${System.currentTimeMillis()}")
    //
    //        val requestInfo = taps.map(x => TopicAndPartition(x.topic(), x.partition()) -> PartitionOffsetRequestInfo(time, 1)).toMap
    //
    //        val offsetRequest = new OffsetRequest(requestInfo)
    //
    //        val offsetResponse = consumer.getOffsetsBefore(offsetRequest)
    //
    //        if (offsetResponse.hasError) {
    //          println(s"get topic offset failed offsetResponse $offsetResponse")
    //          throw new SparkException(s"get topic offset failed $leaderHost $taps")
    //        }
    //        offsetResponse.offsetsGroupedByTopic.values.foreach(partitionToResponse => {
    //          partitionToResponse.foreach {
    //            case (tap, por) => offsetMap.put(new TopicPartition(tap.topic, tap.partition), por.offsets.head)
    //          }
    //        })
    //        Try(consumer.close())
    //    }
  }

  /**
   * 更新 Offset 到指定时间
   *
   * @param groupId
   * @param topics
   * @param time
   */
  def updateOffsetsForTime(groupId: String, topics: Set[String], time: Long): Unit = {
    val offset = getOffsets(topics.toSeq, time)
    updateOffsets(groupId, offset)
  }

  /**
   * 跟新Offset 到 最开始
   *
   * @param groupId
   * @param topics
   */
  def updateOffsetsForEarliest(groupId: String, topics: Set[String]): Unit = {
    val offset = getEarliestOffsets(topics.toSeq)
    updateOffsets(groupId, offset)
  }

  /**
   * 更新Offset 到最后
   *
   * @param groupId
   * @param topics
   */
  def updateOffsetsForLatest(groupId: String, topics: Set[String]): Unit = {
    val offset = getLatestOffsets(topics.toSeq)
    updateOffsets(groupId, offset)
  }


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()

    conf.set("spark.source.kafka.consume.bootstrap.servers", "localhost:9092")
    conf.set("spark.source.kafka.consume.group.id", "mryt_tz_goods_test_v2")
    conf.set("spark.source.kafka.consume.topics", "mryt_tz_goods")
    conf.set("spark.source.kafka.consume.key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    conf.set("spark.source.kafka.consume.value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    conf.set("spark.source.kafka.consume.auto.offset.reset", "earliest")

    val km = new KafkaManager(conf)


  }


}
