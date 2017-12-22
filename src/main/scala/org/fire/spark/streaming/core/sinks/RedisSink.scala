package org.fire.spark.streaming.core.sinks

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.fire.spark.streaming.core.plugins.redis.{RedisConnectionPool, RedisEndpoint}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by cloud on 17/12/14.
  */
class RedisSink[T <: scala.Product : ClassTag : TypeTag](@transient
                                                         override
                                                         val ssc : StreamingContext)
  extends Sink[T]{

  private lazy val redisEndpoint = new RedisEndpoint(sparkConf)

  def output(rdd : RDD[T], time : Time=Time(System.currentTimeMillis())): Unit = {
    rdd.foreachPartition(r => {
      val jedis = RedisConnectionPool.connect(redisEndpoint)
      val pipe = jedis.pipelined()
      r.foreach(d => {
        val vd = d.asInstanceOf[(Any, Any)]
        pipe.set(vd._1.toString, vd._2.toString)
        pipe.expire(vd._1.toString, 3600 * 24 * 7)
      })
      pipe.sync()
      pipe.close()
      jedis.close()
    })
  }
}

object RedisSink {
  def apply(ssc : StreamingContext) = new RedisSink[(String,String)](ssc)
}


/*
class RedisSink(@transient override val ssc : StreamingContext) extends Sink[(String,String)]{
  private lazy val redisEndpoint = new RedisEndpoint(sparkConf)

  override def output(rdd : RDD[(String,String)], time : Time=Time(System.currentTimeMillis())): Unit = {
    rdd.foreachPartition(r => {
      val jedis = RedisConnectionPool.connect(redisEndpoint)
      val pipe = jedis.pipelined()
      r.foreach(d => {
        pipe.set(d._1, d._2)
        pipe.expire(d._1, 3600 * 24 * 7)
      })
      pipe.sync()
      pipe.close()
      jedis.close()
    })
  }
}*/
