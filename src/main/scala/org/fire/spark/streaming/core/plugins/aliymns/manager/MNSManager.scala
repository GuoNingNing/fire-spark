package org.fire.spark.streaming.core.plugins.aliymns.manager

import com.aliyun.mns.client.{CloudAccount, CloudQueue, MNSClient}
import com.aliyun.mns.model.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Partition, SparkConf}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.aliymns.manager.MNSDStream
import org.apache.spark.streaming.dstream.InputDStream
import org.fire.spark.streaming.core.Logging
import org.fire.spark.streaming.core.plugins.aliymns.AliyMNSClientSingleton

import scala.reflect.ClassTag
import scala.collection.JavaConversions._

/**
  * Created by guoning on 2018/5/31.
  *
  *
  */
class MNSManager(val sparkConf: SparkConf) extends Logging with Serializable {

  private var client: MNSClient = _

  private var queueName: String = _

  private lazy val queue = client.getQueueRef(queueName)

  def createMNSStream(ssc: StreamingContext,
                      param: Map[String, String],
                      numPartitions: Int
                     ): InputDStream[Message] = {


    new MNSDStream(ssc, param, numPartitions)
  }

  def createMNSReceiverStream(ssc: StreamingContext,
                              param: Map[String, String],
                              numPartitions: Int = 1,
                              storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY
                             ): InputDStream[Message] = {

    client = AliyMNSClientSingleton.connect(param)

    queueName = param("queue.name")
    new MNSReceiverDStream(ssc, param, numPartitions, storageLevel)
  }

  def updateOffsets(offset: Array[String]): Unit = {

    queue.batchDeleteMessage(offset.toList)
//    if (offset.nonEmpty) {
//      val (a, b) = offset.splitAt(10)
//      queue.batchDeleteMessage(a.toList)
//      updateOffsets(b)
//    }
  }


}
