package org.fire.spark.streaming.core.plugins.redis

import java.util.concurrent.ConcurrentHashMap

import org.slf4j.LoggerFactory
import redis.clients.jedis.exceptions.JedisConnectionException
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

import scala.annotation.meta.getter
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object RedisConnectionPool {

  lazy val logger = LoggerFactory.getLogger(getClass)

  @transient
  @getter
  private lazy val pools: ConcurrentHashMap[RedisEndpoint, JedisPool] =
    new ConcurrentHashMap[RedisEndpoint, JedisPool]()


  /**
    * 创建一个Redis连接池
    *
    * @param params
    *        redis.hosts
    *        redis.port
    *        redis.auth
    *        redis.dbnum
    *        redis.timeout
    * @return
    */
  def connect(params: Map[String, String]): Jedis = {

    val hosts = params.getOrElse("redis.hosts", "localhost").split(",").map(_.trim)
    val port = params.getOrElse("redis.port", "6379").toInt
    val auth = Try(params("redis.auth")) getOrElse null
    val dbNum = params.getOrElse("redis.dbnum", "0").toInt
    val timeout = params.getOrElse("redis.timeout", "2000").toInt

    val endpoints = hosts.map(RedisEndpoint(_, port, auth, dbNum, timeout))

    connect(endpoints)
  }


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
      case e: Exception => logger.error(e.getMessage)
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

  def safeClose[R](f : Jedis => R)(implicit jedis: Jedis) :  R = {
    val result = f(jedis)
    Try{
      jedis.close()
    }match {
      case Success(o) => logger.debug("jedis.close successful.")
      case Failure(o) => logger.error("jedis.close failed.")
    }
    result
  }
}
