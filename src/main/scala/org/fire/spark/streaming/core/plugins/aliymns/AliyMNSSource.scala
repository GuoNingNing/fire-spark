package org.fire.spark.streaming.core.plugins.aliymns

import java.util.concurrent.ConcurrentHashMap

import com.aliyun.mns.model.Message
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.OffsetRange
import org.fire.spark.streaming.core.plugins.aliymns.manager.MNSManager
import org.fire.spark.streaming.core.sources.Source

import scala.reflect.ClassTag

/**
  * <pre>
  *
  * spark.source.aliymns.consume.access.id=
  * spark.source.aliymns.consume.access.key=
  * spark.source.aliymns.consume.endpoint=
  * spark.source.aliymns.consume.queue.name=
  * spark.source.aliymns.consume.batchsize=
  * spark.source.aliymns.consume.waittime=
  * spark.source.aliymns.consume.taskNum=
  *
  * </pre>
  *
  * @param ssc
  * @param specialParams
  */
class AliyMNSSource(@transient val ssc: StreamingContext,
                    specialParams: Map[String, String] = Map.empty[String, String]) extends Source {
  override val paramPrefix: String = "spark.source.aliymns.consume."
  override type SourceType = Message

  // 保存 offset
  private lazy val offsetRanges: java.util.Map[Long, Array[String]] = new ConcurrentHashMap[Long, Array[String]]


  val numPartitions: Int = param.getOrElse("taskNum", "1").toInt
  val mm = new MNSManager(sparkConf)


  /**
    * 获取DStream 流
    *
    * @return
    */
  override def getDStream[R: ClassTag](f: SourceType => R): DStream[R] = {
    mm.createMNSStream(ssc, param, numPartitions).transform((rdd, time) => {
      offsetRanges.put(time.milliseconds, rdd.map(_.getReceiptHandle).collect)
      rdd
    }).map(f)
  }

  def getMNSReceiverDStream[R: ClassTag](f: SourceType => R): DStream[R] = {
    mm.createMNSReceiverStream(ssc, param, numPartitions).transform((rdd, time) => {
      offsetRanges.put(time.milliseconds, rdd.map(_.getReceiptHandle).collect())
      rdd
    }).map(f)
  }

  /**
    * 更新Offset 操作 一定要放在所有逻辑代码的最后
    * 这样才能保证,只有action执行成功后才更新offset
    */
  def updateOffsets(time: Long): Unit = {
    // 更新 offset
    val offset = offsetRanges.get(time)
    logInfo(s"updateOffsets total ${offset.length} for time $time")
    mm.updateOffsets(offset)
    offsetRanges.remove(time)
  }

}
