package org.fire.spark.streaming.core.sinks

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.elasticsearch.spark.rdd.EsSpark

import scala.collection.Map
import scala.language.postfixOps

/**
  * Created by guoning on 16/8/19.
  *
  * 输出Hbase
  *
  * spark.sink.hbase.hbase.master
  * spark.sink.hbase.hbase.zookeeper.quorum
  * spark.sink.hbase.hbase.zookeeper.property.clientPort
  * spark.sink.hbase.hbase.mapred.outputtable
  *
  * @param sc
  * @param buildPut
  * @param initParams
  * @tparam T
  */
class HbaseSink[T](@transient override val sc: SparkContext,
                   buildPut: T => Put,
                   initParams: Map[String, String] = Map.empty[String, String])
  extends Sink[T] {

  override val paramPrefix: String = "spark.sink.hbase."

  private lazy val hbaseParam = param ++ initParams
  private val tableName = hbaseParam("hbase.mapred.outputtable")

  private def configuration = {
    val conf = HBaseConfiguration.create()
    for ((key, value) <- hbaseParam) {
      conf.set(key, value)
    }
    conf
  }

  lazy val hc = new HBaseContext(sc, configuration)

  /**
    *
    * @param rdd
    * @param time
    */
  def output(rdd: RDD[T], time: Time = Time(System.currentTimeMillis())): Unit = {
    hc.bulkPut(rdd, TableName.valueOf(tableName), buildPut)
  }
}
