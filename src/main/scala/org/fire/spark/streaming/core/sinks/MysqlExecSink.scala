package org.fire.spark.streaming.core.sinks

import java.util.Properties

import org.apache.commons.dbutils.QueryRunner
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.Time
import org.fire.spark.streaming.core.plugins.mysql.MysqlConnectPool

import scala.collection.JavaConversions._

/**
  * Created by cloud on 2019/04/19.
  * 此 sink 用于将已拼装好的sql语句在mysql上执行
  */
class MysqlExecSink(@transient override val sc: SparkContext,
                    initParams: Map[String, String] = Map.empty) extends Sink[String] {

  private lazy val prop : Properties = {
    val p = new Properties()
    p.putAll(param ++ initParams)
    p
  }

  override val paramPrefix: String = "spark.sink.mysql.exec."

  private val url = prop.getProperty("url")
  private val user = prop.getProperty("user")
  private val password = prop.getProperty("password")

  override def output(rdd: RDD[String], time: Time): Unit = {
    rdd.foreachPartition(iter => {
      val conn = MysqlConnectPool.connect(url, user, password)
      val queryRunner = new QueryRunner()
      val successCnt = iter.map(sql => queryRunner.update(conn, sql)).sum
      logger.info(s"success exec sql total: $successCnt")
      conn.close()
    })
  }

}
