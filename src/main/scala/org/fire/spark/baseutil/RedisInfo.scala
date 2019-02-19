package org.fire.spark.baseutil

import net.minidev.json.JSONObject
import org.fire.spark.streaming.core.plugins.redis.{RedisConnectionPool, RedisEndpoint}

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created on 2017-10-12.
  * Copyright (c) 2017, ingkee版权所有.
  * Author: shumeng.ren
  */
object RedisInfo {

    // 默认超时时间
    private var timeout = 3600 * 24 * 180
    private var redis_key: String = ""


    // 信息数据存放map
    private val infoMap = mutable.Map[String, String]()
    val a = mutable.Map(("1", "1"),("2", "2"))


    /**
      * 将关键信息存入
      * @param key
      * @param value
      * @return
      */
    def putValue(key:String, value:String) = {
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
    def clear() = {
        infoMap.clear()
        redis_key=""
    }

    /**
      * 设置公共前缀
      * @param prefix
      */
    def setPrefix(prefix:String) = {
        redis_key = prefix
    }

    /**
      * 设置超时时间
      * @param timeout
      */
    def setTimeout(timeout:Int) = {
        this.timeout = timeout
    }


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
    def storeData(host:String,port:Int, password:String) = {
        val redisPoint = RedisEndpoint(host, port, password)
        val pool = RedisConnectionPool.connect(redisPoint)

        println(s"redis_key:$redis_key infomap: ${infoMap}")
        println(s"redis info:$host  $port   $password")

        if (!redis_key.isEmpty) {
            val real_value = JSONObject.toJSONString(infoMap)

            pool.set(redis_key, real_value)
            pool.expire(redis_key, timeout)

        } else {
            println("redis_key is empty!!!!")
        }

        pool.close()
    }
}
