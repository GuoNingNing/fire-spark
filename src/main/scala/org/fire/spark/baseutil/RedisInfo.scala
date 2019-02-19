package org.fire.spark.baseutil

import net.minidev.json.JSONObject
import org.fire.spark.streaming.core.plugins.redis.{RedisConnectionPool, RedisEndpoint}
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created on 2017-10-12.
  * Copyright (c) 2017, ingkee版权所有.
  * Author: shumeng.ren
  * 将一些公共的操作抽离出来，可能特定的业务会使用到
  */
object RedisInfo {

    // 默认超时时间
    private var timeout = 3600 * 24 * 180
    private var redis_key: String = ""

    // 信息数据存放map
    private val infoMap = mutable.Map[String, String]()


    /**
      * 将关键信息存入
      * @param key
      * @param value
      * @return
      */
    def putValue(key:String, value:String): Option[String] = {
        infoMap.put(key, value)
    }

    /**
      * 获取map中的数据
      * @param key
      * @return
      */
    def getValue(key:String):Option[String] = {
        infoMap.get(key)
    }

    /**
      * 清空map
      */
    def clear(): Unit = {
        infoMap.clear()
        redis_key=""
    }

    /**
      * 设置公共前缀
      * @param prefix
      */
    def setPrefix(prefix:String): Unit = {
        redis_key = prefix
    }

    /**
      * 设置超时时间
      * @param timeout
      */
    def setTimeout(timeout:Int): Unit = {
        this.timeout = timeout
    }


    /**
      * 将数据显示出来
      */
    def showData():Unit = {
        println(s"redis key:$redis_key")
        println(s"redis value:${JSONObject.toJSONString(infoMap)}")
    }

    /**
      * 存写数据
      * @param host
      * @param port
      * @param password
      */
    def storeData(host:String, port:Int, password:String): Unit = {
        val redisPoint = RedisEndpoint(host, port, password)
        val client = RedisConnectionPool.connect(redisPoint)

        println(s"redis_key:$redis_key infomap: $infoMap")
        println(s"redis info:$host  $port   $password")

        if (!redis_key.isEmpty) {
            val real_value = JSONObject.toJSONString(infoMap)

            client.set(redis_key, real_value)
            client.expire(redis_key, timeout)

        } else {
            println("redis_key is empty!!!!")
        }
        client.close()
    }
}
