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
                                          val initParams: Map[String, String] = Map.empty[String, String])
  extends Sink[T]{

  override val paramPrefix: String = "spark.sink.hbase."

  private lazy val prop = {
    val p = new Properties()
    p.putAll(param.map { case (k,v) => s"hbase.$k"->v } ++ initParams)
    p
  }

  private val tableName = prop.getProperty("hbase.table")
  private val commitBatch = prop.getProperty("hbase.commit.batch","1000").toInt
  private val bufferSize = prop.getProperty("hbase.commit.buffer",s"${5*1024*1024}").toLong

  private def getTable : BufferedMutator = {
    val conf = HBaseConfiguration.create
    prop.foreach {case (k,v) => conf.set(k,v)}
    val connection = HbaseConnPool.connect(conf)
    val bufferedMutatorParams = new BufferedMutatorParams(TableName.valueOf(tableName))
    bufferedMutatorParams.writeBufferSize(bufferSize)
    bufferedMutatorParams.maxKeyValueSize(commitBatch)
    connection.getBufferedMutator(bufferedMutatorParams)
  }

  /** 输出
    *
    * @param rdd
    * @param time
    */
  override def output(rdd: RDD[T], time: Time = Time(System.currentTimeMillis())): Unit = {
    rdd.foreachPartition { r =>
      val table = getTable

      r.foreach {
        case put: Put => table.mutate(put)
        case del: Delete => table.mutate(del)
      }

      table.flush()
      table.close()
    }
  }

  def close(): Unit = HbaseConnPool.close()

}

object HBaseSink {
  def apply(sc: SparkContext) = new HBaseSink[Put](sc)
  def apply[T <: Mutation : ClassTag](rdd: RDD[T]) = new HBaseSink[T](rdd.sparkContext)
}
