package org.demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.fire.spark.streaming.core.kit.Heartbeat
import org.fire.spark.streaming.core.{FireStreaming, Logging}
import org.fire.spark.streaming.core.plugins.kafka.KafkaDirectSource

object ReadKafkaDemoA extends FireStreaming with Logging {

  private var heartbeat: Heartbeat = _

  /**
    * 初始化，函数，可以设置 sparkConf
    * 初始化自己私有参数的方法,非必需的,看个人需求
    * @param sparkConf
    */
  override def init(sparkConf: SparkConf): Unit = {
    logInfo("init SparkConf")
    sparkConf.set("spark.xxx", "x")

    /**
      * 用于添加在sparkContext启动时注册能接收所有事件的SparkListener
      * 建议直接在配置文件中添加
      */
    addSparkListeners("org.apache.spark.StartSparkAppListener")
  }


  /**
    * StreamingContext 运行之后执行
    */
  override def afterStarted(ssc: StreamingContext): Unit = {
    logInfo("StreamingContext already start.")
    heartbeat = new Heartbeat(ssc)
    heartbeat.start()
  }

  /**
    *  StreamingContext 停止后 程序停止前 执行
    */
  override def beforeStop(ssc: StreamingContext): Unit = {
    if(heartbeat != null){
      heartbeat.stop()
    }
  }

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
