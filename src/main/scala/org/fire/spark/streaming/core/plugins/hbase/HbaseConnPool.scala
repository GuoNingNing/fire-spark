package org.fire.spark.streaming.core.plugins.hbase

import java.util.concurrent.ConcurrentHashMap

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._

/**
  * Created by guoning on 2016/12/30.
  *
  * Kafka Consumer 连接池
  *
  */

object HbaseConnPool {

  private lazy val logger = LoggerFactory.getLogger(getClass)



  @transient
  private lazy val pools: ConcurrentHashMap[Configuration, Connection] = new ConcurrentHashMap[Configuration, Connection]()


  def connect(configuration: Configuration): Connection = {
    pools.getOrElseUpdate(configuration, ConnectionFactory.createConnection(configuration))
  }

  //  def getTable(table: String): Table = {
  //    connect().getTable(TableName.valueOf(table))
  //  }

}
