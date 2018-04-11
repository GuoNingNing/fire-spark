package org.fire.spark.streaming.core.sinks

import java.util.Properties

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Time
import org.apache.hadoop.hbase.client._
import org.apache.spark.rdd.RDD
import org.fire.spark.streaming.core.plugins.hbase.HbaseConnPool

import scala.reflect.ClassTag
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

/**
  * Created by cloud on 18/4/3.
  */
class HBaseSink[T <: Mutation : ClassTag](@transient override val sc: SparkContext,
                               initParams: Map[String, String] = Map.empty[String, String]) extends Sink[T]{

  override val paramPrefix: String = "spark.sink.hbase."

  private lazy val prop = {
    val p = new Properties()
    p.putAll(param ++ initParams)
    p
  }

  private val tableName = prop.getProperty("table")

  private def getTable(table: String) : Table = {
    val conf = HBaseConfiguration.create
    prop.foreach {case (k,v) => conf.set(s"hbase.$k",v)}
    val connection = HbaseConnPool.connect(conf)
    connection.getTable(TableName.valueOf(table))
  }

  /** 输出
    *
    * @param rdd
    * @param time
    */
  override def output(rdd: RDD[T], time: Time = Time(System.currentTimeMillis())): Unit = {
    rdd.foreachPartition { r =>
      val table = getTable(tableName)
      val mutationList = r.toList
      mutationList match {
        case put: List[Put] => table.put(put.asInstanceOf[List[Put]])
        case del: List[Delete] => table.delete(del.asInstanceOf[List[Delete]])
        case _ =>
      }
/*
      val putList = ListBuffer[Put]()
      val delList = ListBuffer[Delete]()
      r.foreach {
        case put: Put => putList += put
        case del: Delete => delList += del
        case _ =>
      }
      if(putList.nonEmpty) table.put(putList.toList)
      if(delList.nonEmpty) table.delete(delList.toList)
      */
      table.close()
    }
  }

  def close(): Unit = HbaseConnPool.close()

}

object HBaseSink {
  def apply(sc: SparkContext) = new HBaseSink[Put](sc)
  def apply[T <: Mutation : ClassTag](rdd: RDD[T]) = new HBaseSink[T](rdd.sparkContext)
}
