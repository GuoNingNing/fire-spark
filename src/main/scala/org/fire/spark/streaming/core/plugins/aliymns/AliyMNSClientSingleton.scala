package org.fire.spark.streaming.core.plugins.aliymns

import java.util.concurrent.ConcurrentHashMap

import com.aliyun.mns.client.{CloudAccount, MNSClient}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.fire.spark.streaming.core.SQLContextSingleton.instance
import org.fire.spark.streaming.core.plugins.redis.RedisEndpoint
import redis.clients.jedis.JedisPool
import scala.collection.JavaConversions._

import scala.annotation.meta.getter

/**
  * Created by guoning on 2018/5/31.
  *
  */
object AliyMNSClientSingleton {

  @transient
  @getter
  private lazy val pools: ConcurrentHashMap[String, MNSClient] =
    new ConcurrentHashMap[String, MNSClient]()

  def connect(param: Map[String, String]): MNSClient = {

    val accessId: String = param("access.id")
    val accessKey: String = param("access.key")
    val endpoint: String = param("endpoint")

    pools.getOrElseUpdate(s"$accessId#$accessKey#$endpoint", new CloudAccount(accessId, accessKey, endpoint).getMNSClient)

  }


}
