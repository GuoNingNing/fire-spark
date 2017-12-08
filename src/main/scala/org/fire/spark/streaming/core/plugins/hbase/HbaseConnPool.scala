package org.fire.spark.streaming.core.plugins.hbase

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.spark.SparkConf

import scala.collection.JavaConversions._

/**
  * Created by guoning on 2017/10/18.
  *
  * Hbase 连接池
  */
object HbaseConnPool {


  @transient
  private lazy val pools: ConcurrentHashMap[Configuration, Connection] = new ConcurrentHashMap[Configuration, Connection]()


  def connect(params: Map[String, String]): Connection = {
    val conf = HBaseConfiguration.create
    for ((key, value) <- params) {
      conf.set(key, value)
    }
    connect(conf)
  }

  def connect(sparkConf: SparkConf): Connection = {

    val conf = HBaseConfiguration.create

    for ((key, value) <- sparkConf.getAllWithPrefix("spark.hbase")) {
      conf.set(key.substring(6), value)
    }

    connect(conf)
  }

  def connect(conf: Configuration): Connection = {
    pools.getOrElseUpdate(conf, ConnectionFactory.createConnection(conf))
  }

}