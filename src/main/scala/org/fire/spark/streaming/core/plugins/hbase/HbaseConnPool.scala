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
  private lazy val pools: ConcurrentHashMap[String, Connection] = new ConcurrentHashMap[String, Connection]()


  def connect(params: Map[String, String]): Connection = {
    val conf: Configuration = HBaseConfiguration.create
    for ((key, value) <- params) {
      conf.set(key, value)
    }
    connect(conf)
  }

  /*
  * spark.hbase.hbase.zookeeper.quorum=ip1,ip2,ip3
  * spark.hbase.hbase.master=ip:port
  * */
  def connect(sparkConf: SparkConf): Connection = {

    val conf: Configuration = HBaseConfiguration.create

    for ((key, value) <- sparkConf.getAllWithPrefix("spark.hbase.")) {
      conf.set(key, value)
    }

    connect(conf)
  }

  def connect(conf: Configuration): Connection = {
    val zookeeper = conf.get("hbase.zookeeper.quorum")
    pools.getOrElseUpdate(zookeeper, ConnectionFactory.createConnection(conf))
  }

  def close(): Unit = pools.foreach { case (k, v) => v.close() }

}