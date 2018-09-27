package org.fire.spark.streaming.core.plugins.redis

import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkConf
import org.fire.spark.streaming.core.plugins.redis.RedisClusterPipeline.RedisCmd
import redis.clients.jedis._
import redis.clients.jedis.exceptions.{JedisAskDataException, JedisConnectionException, JedisDataException, JedisMovedDataException}
import redis.clients.util.{JedisClusterCRC16, SafeEncoder}

import scala.collection.JavaConversions._
import scala.util.Try

/**
  * Created by cloud on 18/9/18.
  * redisCluster pipe 通过把key提前按slot分组,再在各slot主机上进行pipe调用
  * 通过返回值列表判断命令是否需要moved和ask然后在进行尝试
  * 执行结果返回List[String]
  * 不支持线程安全
  */
class RedisClusterPipeline(val sparkConf: SparkConf) {

  val redisHost = sparkConf.get("spark.redis.host")
  val redisPort = sparkConf.get("spark.redis.port").toInt
  val redisPassword = sparkConf.get("spark.redis.password")

  val hosts = Collections.singleton(RedisClusterPipeline.getHostAndPort(redisHost,redisPort))
  val poolConfig = RedisClusterPipeline.getJedisPoolConfig
  val connectionHandler = new JedisSlotBasedConnectionHandler(hosts, poolConfig, 30000, 30000, redisPassword)

  val response = new ConcurrentHashMap[Int,(String, Int)]()
  val command = new ConcurrentHashMap[Int, List[RedisCmd]]()

  def cmdExec(redisCmd: RedisCmd, pipe: Pipeline): Unit = {
    redisCmd.cmd.toUpperCase match {
      case "SET" => pipe.set(redisCmd.key, redisCmd.params(0))
      case "LPUSH" => pipe.lpush(redisCmd.key, redisCmd.params(0))
      case "GET" => pipe.get(redisCmd.key)
      case "EXPIRE" => pipe.expire(redisCmd.key, redisCmd.params(0).toInt)
      case _ => //No-op
    }
  }

  def set(k: String, v: String): Unit = {
    putCmd("set",k, Array(v))
  }

  def get(k: String): Unit = {
    putCmd("get", k, Array.empty[String])
  }

  def expire(k: String, exp: Int): Unit = {
    putCmd("expire", k, Array(s"$exp"))
  }

  def lpush(k: String, v: String): Unit = {
    putCmd("lpush", k, Array(v))
  }

  def putCmd(cmd: String, k: String, v: Array[String]): Unit = {
    val cmdList = command.getOrDefault(JedisClusterCRC16.getSlot(k), List.empty[RedisCmd])
    command.put(JedisClusterCRC16.getSlot(k), cmdList :+ RedisCmd(cmd,k,v))
  }

  def execute(): List[String] = {
    val res = command.flatMap { case (k, v) =>
      val jedis = connectionHandler.getConnectionFromSlot(k)
      askAndMoved(v, jedis)
    }.toList
    command.clear()
    res
  }

  private def askAndMoved(cmds: List[RedisCmd], conn: Jedis): Array[String] = {
    try {
      val result = new Array[String](cmds.length)
      val res = pipeExec(cmds, conn)
      writeRes(res, result)
      val isMoved = res.exists(_.isInstanceOf[JedisMovedDataException])
      val moved = res.zipWithIndex.flatMap(s => parseAskAndMoved(s._1, s._2))
        .map(x => x._2 -> (x._1, cmds(x._1))).groupBy(s => s._1).map(s => s._1 -> s._2.map(_._2))
      if(isMoved) {
        connectionHandler.renewSlotCache()
      }
      moved.nonEmpty match {
        case true =>
          moved.foreach { case (h, l) =>
              val r = pipeExec(l.map(_._2), connectionHandler.getConnectionFromNode(h))
              writeRes(r, result, l.map(_._1))
          }
        case false => //No-op
      }
      result
    } catch {
      case e: JedisConnectionException => Array(e.getMessage)
      case _ => Array.empty[String]
    }
  }

  private def pipeExec(cmds: List[RedisCmd], conn: Jedis): List[AnyRef] = {
    try {
      val p = conn.pipelined()
      cmds.foreach(cmdExec(_, p))
      p.syncAndReturnAll().toList
    } catch {
      case e: JedisConnectionException => List(e)
      case _ => List.empty[AnyRef]
    }
  }

  private def writeRes(response: List[AnyRef], result: Array[String], indexs: List[Int] = List.empty): Unit = {
    indexs.isEmpty match {
      case true =>
        response
          .zipWithIndex
          .filter(_._1.isInstanceOf[Array[Byte]])
          .map(s => s._2 -> SafeEncoder.encode(s._1.asInstanceOf[Array[Byte]]))
          .foreach(x => result(x._1) = x._2)

      case false =>
        indexs.foreach { i =>
          val r = Try(SafeEncoder.encode(response(i).asInstanceOf[Array[Byte]])).getOrElse("")
          result(i) = r
        }
    }
  }

  private def parseAskAndMoved(res: AnyRef,index: Int): Option[(Int,HostAndPort)] = {
    res match {
      case ask: JedisAskDataException =>
        val hp = targetHostAndPort(ask.getMessage)
        Some(index -> new HostAndPort(hp(0), hp(1).toInt))
      case moved: JedisMovedDataException =>
        val hp = targetHostAndPort(moved.getMessage)
        connectionHandler.renewSlotCache()
        Some(index -> new HostAndPort(hp(0), hp(1).toInt))
      case data: JedisDataException => None
      case _ => None
    }
  }

  private def targetHostAndPort(msg: String): Array[String] = {
    val mi = msg.split(" ")
    mi(2).split(":")
  }

  def close(): Unit = {
    connectionHandler.close()
  }

}

object RedisClusterPipeline {

  case class RedisCmd(cmd: String, key: String, params: Array[String])

  def getHostAndPort(host: String, port: Int): HostAndPort = new HostAndPort(host, port)

  def getJedisPoolConfig: JedisPoolConfig = {
    val poolConfig: JedisPoolConfig = new JedisPoolConfig()
    poolConfig.setMaxTotal(1000)
    poolConfig.setMaxIdle(64)
    poolConfig.setTestOnBorrow(true)
    poolConfig.setTestOnReturn(false)
    poolConfig.setMinEvictableIdleTimeMillis(180000)
    poolConfig.setTimeBetweenEvictionRunsMillis(30000)
    poolConfig.setNumTestsPerEvictionRun(-1)
    poolConfig
  }
}