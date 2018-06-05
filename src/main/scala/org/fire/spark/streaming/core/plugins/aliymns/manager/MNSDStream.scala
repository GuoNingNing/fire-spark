package org.apache.spark.streaming.aliymns.manager

import com.aliyun.mns.client.{CloudAccount, MNSClient}
import com.aliyun.mns.model.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.fire.spark.streaming.core.plugins.aliymns.manager.MNSRDD

import scala.reflect.ClassTag

/**
  * Created by guoning on 2018/5/31.
  *
  */
class MNSDStream(ssc: StreamingContext,
                 param: Map[String, String],
                 numPartitions: Int) extends InputDStream[Message](ssc) with Logging {


  override def start(): Unit = {}

  override def stop(): Unit = {}

  override def compute(validTime: Time): Option[RDD[Message]] = {

    val rdd = new MNSRDD(ssc.sparkContext, param, numPartitions)

    val metadata = Map(StreamInputInfo.METADATA_KEY_DESCRIPTION -> "Pull the MNS data ...")
    val inputInfo = StreamInputInfo(id, rdd.count, metadata)
    ssc.scheduler.inputInfoTracker.reportInfo(validTime, inputInfo)

    Some(rdd)
  }
}
