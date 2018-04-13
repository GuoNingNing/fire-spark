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
import scala.collection.mutable.ArrayBuffer

/**
  * Created by cloud on 18/4/3.
  */
class HBaseSink[T <: Mutation : ClassTag](@transient override val sc: SparkContext,
                                           initParams: Map[String, String] = Map.empty[String, String])
  extends Sink[T]{

  override val paramPrefix: String = "spark.sink.hbase."

  private lazy val prop = {
    val p = new Properties()
    p.putAll(param.map { case (k,v) => s"hbase.$k"->v } ++ initParams)
    p
  }

  private val tableName = prop.getProperty("hbase.table")
  private val commitBatch = prop.getProperty("hbase.commit.batch","1000").toInt

  private def getTable(table: String) : Table = {
    val conf = HBaseConfiguration.create
    prop.foreach {case (k,v) => conf.set(k,v)}
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
      var putList: ArrayBuffer[Put] = new ArrayBuffer[Put]()
      var delList: ArrayBuffer[Delete] = new ArrayBuffer[Delete]()

      r.foreach {
        case put: Put =>
          putList += put
          if(putList.length > commitBatch){
            table.put(putList)
            putList = new ArrayBuffer[Put]()
          }
        case del: Delete =>
          delList += del
          if(delList.length > commitBatch){
            table.delete(delList)
            delList = new ArrayBuffer[Delete]()
          }
      }
      if(putList.nonEmpty) table.put(putList)
      if(delList.nonEmpty) table.delete(delList)

      table.close()
    }
  }

  def close(): Unit = HbaseConnPool.close()

}

object HBaseSink {
  def apply(sc: SparkContext) = new HBaseSink[Put](sc)
  def apply[T <: Mutation : ClassTag](rdd: RDD[T]) = new HBaseSink[T](rdd.sparkContext)
}
