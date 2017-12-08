package org.fire.spark.streaming.core.sources

import java.util.concurrent.ConcurrentHashMap

import kafka.message.MessageAndMetadata
import kafka.serializer.Decoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaManager, OffsetRange}

import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.util.Try

/**
  * Created by guoning on 2017/5/25.
  * 封装 Kafka Direct Api
  *
  * @param ssc
  * @param specialKafkaParams
  */
class KafkaDirectSource[K: ClassTag, V: ClassTag, KD <: Decoder[K] : ClassTag, VD <: Decoder[V] : ClassTag](@transient val ssc: StreamingContext,
                                                                                                            specialKafkaParams: Map[String, String] = Map.empty[String, String])
  extends Source {
  override val paramPrefix: String = "spark.source.kafka.consume."

  // 保存 offset
  private lazy val offsetRanges: java.util.Map[Long, Array[OffsetRange]] = new ConcurrentHashMap[Long, Array[OffsetRange]]

  // 分区数
  private lazy val repartition: Int = sparkConf.get("spark.source.kafka.consume.repartition", "0").toInt

  // kafka 消费 topic
  private lazy val topicSet: Set[String] = specialKafkaParams.getOrElse("consume.topics",
    sparkConf.get("spark.source.kafka.consume.topics")).split(",").map(_.trim).toSet

  // 组装 Kafka 参数
  private lazy val kafkaParams: Map[String, String] = {
    sparkConf.getAll.flatMap {
      case (k, v) if k.startsWith(paramPrefix) && Try(v.nonEmpty).getOrElse(false) => Some(k.substring(27) -> v)
      case _ => None
    } toMap
  } ++ specialKafkaParams

  private lazy val groupId = kafkaParams.get("group.id")

  override type SourceType = MessageAndMetadata[K, V]


  val km = new KafkaManager(kafkaParams)

  /**
    * 通过指定messageHandler 获得Kafka 数据流
    *
    * @param messageHandler
    * @return
    */
  override def getDStream[R: ClassTag](messageHandler: MessageAndMetadata[K, V] => R): DStream[R] = {

    km.createDirectStream[K, V, KD, VD, R](ssc, kafkaParams, topicSet, messageHandler)
      .transform((rdd, time) => {
        offsetRanges.put(time.milliseconds, rdd.asInstanceOf[HasOffsetRanges].offsetRanges)
        if (repartition > 0) rdd.repartition(repartition) else rdd
      })
  }

  /**
    * 更新Offset 操作 一定要放在所有逻辑代码的最后
    * 这样才能保证,只有action执行成功后才更新offset
    */
  def updateZKOffsets(time: Long): Unit = {
    // 更新 offset
    if (groupId.isDefined) {
      println(s"offsetRanges: $offsetRanges")
      val offset = offsetRanges.get(time)
      km.updateZKOffsets(groupId.get, offset)
      offsetRanges.remove(time)
    }
  }
}