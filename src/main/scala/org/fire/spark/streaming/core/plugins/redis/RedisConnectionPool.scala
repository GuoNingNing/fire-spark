package org.fire.spark.streaming.core.plugins.redis

import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.Logging
import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import scala.collection.JavaConversions._

object RedisConnectionPool extends Logging {
  @transient private lazy val pools: ConcurrentHashMap[RedisEndpoint, JedisPool] =
    new ConcurrentHashMap[RedisEndpoint, JedisPool]()

  /**
    * 随机选择一个 RedisEndpoint 创建 或者获取一个Redis 连接池
    *
    * @param res
    * @return
    */
  def connect(res: Array[RedisEndpoint]): Jedis = {
    assert(res.length > 0, "The RedisEndpoint array is empty!!!")
    val rnd = scala.util.Random.nextInt().abs % res.length
    try {
      connect(res(rnd))
    } catch {
      case e: Exception => logWarning(e.getMessage)
        connect(res.drop(rnd))
    }
  }

  /**
    * 创建或者获取一个Redis 连接池
    *
    * @param re
    * @return
    */
  def connect(re: RedisEndpoint): Jedis = {
    val pool = pools.getOrElseUpdate(re, createJedisPool(re))
    var sleepTime: Int = 4
    var conn: Jedis = null
    while (conn == null) {
      try {
        conn = pool.getResource
      } catch {
        case e: JedisConnectionException if e.getCause.toString.
          contains("ERR max number of clients reached") => {
          if (sleepTime < 500) sleepTime *= 2
          Thread.sleep(sleepTime)
        }
        case e: Exception => throw e
      }
    }
    conn
  }

  /**
    * 创建一个连接池
    *
    * @param re
    * @return
    */
  def createJedisPool(re: RedisEndpoint): JedisPool = {

    println(s"createJedisPool with $re ")
    val poolConfig: JedisPoolConfig = new JedisPoolConfig()
    /*最大连接数*/
    poolConfig.setMaxTotal(1000)
    /*最大空闲连接数*/
    poolConfig.setMaxIdle(64)
    /*在获取连接的时候检查有效性, 默认false*/
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(false)
    /*在空闲时检查有效性, 默认false*/
    poolConfig.setTestWhileIdle(false)
    /*逐出连接的最小空闲时间 默认1800000毫秒(30分钟)*/
    poolConfig.setMinEvictableIdleTimeMillis(1800000)
    /*逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1*/
    poolConfig.setTimeBetweenEvictionRunsMillis(30000)
    poolConfig.setNumTestsPerEvictionRun(-1)
    new JedisPool(poolConfig, re.host, re.port, re.timeout, re.auth, re.dbNum)

  }
}
