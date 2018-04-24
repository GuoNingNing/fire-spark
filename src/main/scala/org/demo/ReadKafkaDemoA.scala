package org.demo

import org.apache.spark.{SparkConf, SparkContext, SparkInternal}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.fire.spark.streaming.core.kit.Heartbeat
import org.fire.spark.streaming.core.{FireStreaming, Logging}
import org.fire.spark.streaming.core.plugins.kafka.KafkaDirectSource
import org.fire.spark.streaming.core.plugins.redis.RedisConnectionPool

import scala.reflect.ClassTag

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
      val myRDD = myFlatMapPartitions(rdd.sparkContext,myFilterPartitions(rdd.sparkContext,rdd))
      myRDD.take(10).foreach(println)
      source.updateOffsets(time.milliseconds)
    })
  }

  /**
    * 如何实现自定义的filterPartitions
    * filter 底层使用的也是MapPartitionsRDD实现的
    * 这里是模仿mapPartitions的方法实现的示例
    *
    * 不建议如此实现,此处只作为调研示例
    * 使用rdd现有的mapPartitions也能实现相同的功能
    *
    * rdd.mapPartitions {
    *   it =>
    *     ... 全局的一些动作
    *     it.filter(...)
    * }
    *
    */
  private def myFilterPartitions(sparkContext: SparkContext,
                                 rdd: RDD[String]): RDD[String] = SparkInternal.withScope(sparkContext){
    val filter = (iterator: Iterator[String]) => {
      /**
        * 这里就是写在Partitions中的处理代码
        */
      val redis = RedisConnectionPool.connect(Map("host" -> "127.0.0.1"))
      iterator.filter { d =>
        val v = redis.get(d)
        v != ""
      }
    }
    val cleanFilter = SparkInternal.clean(filter)
    SparkInternal.newMapPartitionsRDD[String,String](
      rdd,
      (context,index,iterator) => cleanFilter(iterator),
      preservesPartitioning = true
    )
  }

  /**
    * 如何实现自定义的flatMapPartitions
    */
  private def myFlatMapPartitions(sc: SparkContext,
                                  rdd: RDD[String]): RDD[String] = SparkInternal.withScope(sc){
    val flatMap = (it: Iterator[String]) => {
      val redis = RedisConnectionPool.connect(Map.empty[String,String])
      it.flatMap { d =>
        val v = redis.get(d)
        v.split("\t")
      }
    }
    val cleanFunc = SparkInternal.clean(flatMap)
    SparkInternal.newMapPartitionsRDD[String,String](rdd, (context,index,it) => cleanFunc(it))
  }
}
