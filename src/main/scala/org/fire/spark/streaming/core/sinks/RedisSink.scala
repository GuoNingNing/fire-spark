package org.fire.spark.streaming.core.sinks

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.fire.spark.streaming.core.plugins.redis.{RedisConnectionPool, RedisEndpoint}
import org.fire.spark.streaming.core.plugins.redis.RedisConnectionPool._
import redis.clients.jedis.Protocol

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by cloud on 17/12/14.
  *
  *
  */
class RedisSink[T <: scala.Product : ClassTag : TypeTag](@transient override val sc: SparkContext,
                                                         initParams: Map[String, String] = Map.empty[String, String])
  extends Sink[T] {

  override val paramPrefix: String = "spark.sink.redis."

  private lazy val prop = {
    val p = new Properties()
    p.putAll(param ++ initParams)
    p
  }


  private lazy val host = prop.getProperty("host", Protocol.DEFAULT_HOST)
  private lazy val port = prop.getProperty("port", Protocol.DEFAULT_PORT.toString).toInt
  private lazy val auth = prop.getProperty("auth", null)
  private lazy val db = prop.getProperty("db", Protocol.DEFAULT_DATABASE.toString).toInt
  private lazy val timeout = prop.getProperty("timeout", Protocol.DEFAULT_TIMEOUT.toString).toInt


  private val redisEndpoint = RedisEndpoint(host, port, auth, db, timeout)

  def output(rdd: RDD[T], time: Time = Time(System.currentTimeMillis())): Unit = {
    rdd.foreachPartition(r => {
      safeClosePipe { pipe =>
        r.foreach(d => {
          val (k, v, t) = d match {
            case n: (String, Any) => (n._1, n._2.toString, 3600 * 24 * 7)
            case n: (String, Any, Int) => (n._1, n._2.toString, n._3)
            case _ =>
              logger.warn("data type error. key is unknown")
              ("UNKNOWN", d.toString, 60)
          }
          pipe.set(k, v)
          pipe.expire(k, t)
        })
      }(RedisConnectionPool.connect(redisEndpoint))
    })
  }

  def close(): Unit = RedisConnectionPool.close()

}

object RedisSink {
  def apply(sc: SparkContext) = new RedisSink[(String, String)](sc)

  def apply[T <: scala.Product : ClassTag : TypeTag](rdd: RDD[T]) = new RedisSink[T](rdd.sparkContext)
}

