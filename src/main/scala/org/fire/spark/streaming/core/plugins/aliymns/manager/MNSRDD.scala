package org.fire.spark.streaming.core.plugins.aliymns.manager

import com.aliyun.mns.model.Message
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.fire.spark.streaming.core.plugins.aliymns.AliyMNSClientSingleton

import scala.annotation.meta.getter
import scala.collection.JavaConversions._

/**
  * Created by guoning on 2018/5/31.
  *
  */
class MNSRDD(sc: SparkContext,
             param: Map[String, String],
             numPartitions: Int
            ) extends RDD[Message](sc, Nil) with Logging {


  val batch_size: Int = param("batchsize").toInt
  val wait_time: Int = param("waittime").toInt


  private lazy val queueName = param("queue.name")
  @transient
  @getter
  private lazy val queue = AliyMNSClientSingleton.connect(param).getQueueRef(queueName)

  override def compute(split: Partition, context: TaskContext): Iterator[Message] = {
    queue.batchPopMessage(batch_size, wait_time).toIterator
  }

  override protected def getPartitions: Array[Partition] = {
    (0 until numPartitions).map { inx =>
      new MnsPartition(inx)
    }.toArray
  }
}

private[aliymns] class MnsPartition(inx: Int) extends Partition {
  override def index: Int = inx
}
