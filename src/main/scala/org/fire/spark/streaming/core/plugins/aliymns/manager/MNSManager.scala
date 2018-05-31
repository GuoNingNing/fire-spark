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

import scala.reflect.ClassTag
import scala.collection.JavaConverters._

/**
  * Created by guoning on 2018/5/31.
  *
  *
  */
class MNSManager(val sparkConf: SparkConf) extends Logging with Serializable {


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


    new MNSReceiverDStream(ssc, param, numPartitions, storageLevel)
  }


}
