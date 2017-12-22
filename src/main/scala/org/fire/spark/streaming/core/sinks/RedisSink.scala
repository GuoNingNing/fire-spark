package org.fire.spark.streaming.core.sinks

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import org.fire.spark.streaming.core.plugins.redis.{RedisConnectionPool, RedisEndpoint}
import redis.clients.jedis.Protocol

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by cloud on 17/12/14.
  *
  * 暂不支持checkpoint模式
  */
class RedisSink[T <: scala.Product : ClassTag : TypeTag](@transient
                                                         override
                                                         val ssc : StreamingContext)
  extends Sink[T]{
  private lazy val host = sparkConf.get("spark.sink.redis.host",Protocol.DEFAULT_HOST)
  private lazy val port = sparkConf.getInt("spark.sink.redis.port",Protocol.DEFAULT_PORT)
  private lazy val auth = sparkConf.get("spark.sink.redis.auth",null)
  private lazy val db = sparkConf.getInt("spark.sink.redis.db",Protocol.DEFAULT_DATABASE)
  private lazy val timeout = sparkConf.getInt("spark.sink.redis.timeout",Protocol.DEFAULT_TIMEOUT)


  private val redisEndpoint = RedisEndpoint(host,port,auth,db,timeout)

  def output(rdd : RDD[T], time : Time=Time(System.currentTimeMillis())): Unit = {
    rdd.foreachPartition(r => {
      val jedis = RedisConnectionPool.connect(redisEndpoint)
      val pipe = jedis.pipelined()
      r.foreach(d => {
        val (k,v,t) = d match {
          case n : (String,Any) => (n._1,n._2.toString,3600*24*7)
          case n : (String,Any,Int) => (n._1,n._2.toString,n._3)
          case _ => ("UNKNOWN",d.toString,60)
        }
        pipe.set(k, v)
        pipe.expire(k,t)
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
