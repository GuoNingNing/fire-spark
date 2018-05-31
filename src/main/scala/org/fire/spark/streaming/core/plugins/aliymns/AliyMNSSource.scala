package org.fire.spark.streaming.core.plugins.aliymns

import com.aliyun.mns.model.Message
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
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


  val numPartitions: Int = param.getOrElse("taskNum", "1").toInt
  val mm = new MNSManager(sparkConf)


  /**
    * 获取DStream 流
    *
    * @return
    */
  override def getDStream[R: ClassTag](f: SourceType => R): DStream[R] = {
    mm.createMNSStream(ssc, param, numPartitions).map(f)
  }

}
