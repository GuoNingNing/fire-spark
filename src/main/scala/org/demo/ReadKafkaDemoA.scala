package org.demo

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.fire.spark.streaming.core.FireStreaming
import org.fire.spark.streaming.core.plugins.kafka.KafkaDirectSource

object ReadKafkaDemoA extends FireStreaming {


  /* 初始化，函数，可以设置 sparkConf
    * 初始化自己私有参数的方法,非必需的,看个人需求
    * @param sparkConf
    */
  override def init(sparkConf: SparkConf): Unit = {
    sparkConf.set("spark.xxx","x")
  }

  /*
  * 用于添加在sparkContext启动时注册能接收所有事件的listener
  * 非必需的,看需要使用
  * */
  addAllEventListeners("org.apache.spark.StartSparkAppListener")

  /**
    * 处理函数
    *
    * @param ssc
    */
  override def handle(ssc: StreamingContext): Unit = {

    val source = new KafkaDirectSource[String, String](ssc)

    val logs = source.getDStream[String](_.value())

    logs.foreachRDD((rdd, time) => {
      rdd.take(10).foreach(println)
      source.updateOffsets(time.milliseconds)
    })


  }
}
