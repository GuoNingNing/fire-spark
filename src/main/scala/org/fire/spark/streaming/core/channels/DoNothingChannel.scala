package org.fire.spark.streaming.core.channels

import org.apache.spark.streaming.dstream.DStream

/**
  * Created by guoning on 16/8/4.
  *
  * 不做任何事情
  *
  */
class DoNothingChannel extends Channel[String, String] {
  /**
    * 处理
    *
    * @param dStream
    * @return
    */
  override def procese(dStream: DStream[String]): DStream[String] = {
    dStream
  }
}
