package org.fire.spark.streaming.core.plugins.aliymns.manager

import com.aliyun.mns.client.{AsyncCallback, MNSClient}
import com.aliyun.mns.model.Message
import org.apache.curator.utils.ThreadUtils
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.scheduler.StreamInputInfo
import org.apache.spark.streaming.{StreamingContext, Time}
import org.fire.spark.streaming.core.plugins.aliymns.AliyMNSClientSingleton

import scala.util.control.NonFatal

/**
  * Created by guoning on 2018/5/31.
  *
  */
class MNSReceiverDStream(ssc: StreamingContext,
                         param: Map[String, String],
                         numPartitions: Int,
                         storageLevel: StorageLevel) extends ReceiverInputDStream[Message](ssc) with Logging {
  override def getReceiver(): Receiver[Message] = {
    new MNSReceiver(param, numPartitions, storageLevel)
  }
}

private[this] class MNSReceiver(param: Map[String, String],
                                numPartitions: Int,
                                storageLevel: StorageLevel) extends Receiver[Message](storageLevel) with Logging {

  logInfo(s"create new MNSReceiver with param $param")

  private var client: MNSClient = _
  private lazy val queueName = param("queue.name")
  private lazy val wait_time: Int = param("waittime").toInt

  override def onStart(): Unit = {

    logInfo(s"MNSReceiver start ...")

    client = AliyMNSClientSingleton.connect(param)

    val executorPool = ThreadUtils.newFixedThreadPool(numPartitions, "MNSReceiver Streaming")

    try {
      (0 to numPartitions).foreach { index =>
        logInfo(s"MessageHandler submit ... ")
        executorPool.submit(new MessageHandler(index))
      }
    } finally {
      executorPool.shutdown()
    }

  }

  override def onStop(): Unit = {
    logInfo(s"MNSReceiver stop ...")
    try {
      if (null != client) client.close()
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        logError(s"Failed to close MNSClient...")
    }
  }

  private class MessageHandler(index: Int) extends Runnable {
    def run() {
      try {
        val queue = client.getQueueRef(queueName)
        while (!isStopped) {
          val msg = queue.popMessage(wait_time)
          if (msg != null) {
            store(msg)
          }
        }
      } catch {
        case e: Throwable =>
          e.printStackTrace()
          restart("Error receiving data ", e)
      } finally {
        onStop()
      }
    }
  }

}