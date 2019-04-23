package org.fire.spark.streaming.core.plugins.mysql

import java.sql.{Connection, SQLException}
import java.util.concurrent.ConcurrentHashMap

import org.apache.commons.dbcp.BasicDataSource
import org.apache.spark.SparkConf
import org.fire.spark.streaming.core.Logging

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

/**
  * Created by cloud on 2019/04/19.
  */
object MysqlConnectPool extends Logging {

  private val pools: ConcurrentHashMap[Int, BasicDataSource] = new ConcurrentHashMap[Int, BasicDataSource]()

  def connect(mysqlEndpoint: MysqlEndpoint, attemptTotal: Int, attempt: Int = 1): Connection = {
    val ds = pools.getOrElseUpdate(mysqlEndpoint.hashCode(), createSource(mysqlEndpoint))
    Try(ds.getConnection) match {
      case Success(conn) => conn

      case Failure(e) => if (attempt > attemptTotal) throw new SQLException("get connect failed.", e) else {
        Thread.sleep(50)
        connect(mysqlEndpoint, attemptTotal, attempt + 1)
      }
    }
  }

  def connect(parameter: Map[String, String]): Connection = {
    val mysqlEndpoint = MysqlEndpoint(parameter("url"),parameter("user"),parameter("password"))
    connect(mysqlEndpoint)
  }

  def connect(url: String, user: String, password: String): Connection = connect(MysqlEndpoint(url,user,password))

  def connect(mysqlEndpoint: MysqlEndpoint): Connection = connect(mysqlEndpoint, 3)

  def getDataSource(mysqlEndpoint: MysqlEndpoint): BasicDataSource = {
    pools.getOrElseUpdate(mysqlEndpoint.hashCode(), createSource(mysqlEndpoint))
  }

  def createSource(mysqlEndpoint: MysqlEndpoint): BasicDataSource = {
    val source = new BasicDataSource()
    source.setDriverClassName(mysqlEndpoint.className)
    source.setUrl(mysqlEndpoint.url)
    source.setUsername(mysqlEndpoint.user)
    source.setPassword(mysqlEndpoint.password)
    source.setMaxActive(mysqlEndpoint.maxActive)
    source.setInitialSize(1)
    source.setMaxIdle(3)
    source.setMinIdle(1)
    source.setMaxWait(3000)
    source
  }

  def close(): Unit = pools.foreach(_._2.close)

  def safeClose[R](f: Connection => R)(implicit conn: Connection): R = {
    val result = f(conn)
    try {
      conn.close()
    } catch {
      case NonFatal(e) => logWarning("close connection failed.", e)
    }
    result
  }

}

/**
  * @param className driver类
  * @param url       数据库url
  * @param user      数据库用户名
  * @param password  数据库密码
  * @param maxActive 最大连接数
  */
case class MysqlEndpoint(className: String,
                         url: String,
                         user: String,
                         password: String,
                         maxActive: Int)

object MysqlEndpoint {
  def apply(url: String, user: String, password: String): MysqlEndpoint =
    new MysqlEndpoint("com.mysql.jdbc.Driver", url, user, password, 100)

  def apply(sparkConf: SparkConf): MysqlEndpoint = {
    val _prefix = "spark.mysql.pool"
    val className = sparkConf.get(s"${_prefix}.driver.class", "com.mysql.jdbc.Driver")
    val url = sparkConf.get(s"${_prefix}.connect.url")
    val user = sparkConf.get(s"${_prefix}.connect.user")
    val password = sparkConf.get(s"${_prefix}.connect.password")
    val maxActive = sparkConf.getInt(s"${_prefix}.connect.max", 100)
    new MysqlEndpoint(className, url, user, password, maxActive)
  }
}
