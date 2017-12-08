package org.fire.spark.streaming.core.sinks

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.{StreamingContext, Time}
import org.fire.spark.streaming.core.SQLContextSingleton

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
  * Created by guoning on 16/8/3.
  *
  * Mysql Sink
  *
  * 序列化有问题,暂不支持 checkpoint
  *
  */
class MysqlSink[T <: scala.Product : ClassTag : TypeTag](override
                                                         val ssc: StreamingContext)
  extends Sink[T] {


  private lazy val url = sparkConf.get("spark.sink.mysql.url")
  private lazy val table = sparkConf.get("spark.sink.mysql.output_table_name")
  private lazy val saveMode =
    sparkConf.get("spark.sink.mysql.saveMode", "append")
      .toLowerCase() match {
      case "overwrite" => SaveMode.Overwrite
      case "errorifexists" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
      case _ => SaveMode.Append
    }
  val prop = new Properties()
  prop.put("driver", sparkConf.get("spark.sink.mysql.driver", "com.mysql.jdbc.Driver"))
  prop.put("dbtable", sparkConf.get("spark.sink.mysql.dbtable"))
  prop.put("user", sparkConf.get("spark.sink.mysql.user"))
  prop.put("password", sparkConf.get("spark.sink.mysql.password"))


  /**
    * 输出 到 Mysql
    *
    * @param rdd
    * @param time
    */
  override def output(rdd: RDD[T], time: Time): Unit = {
    val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
    import sqlContext.implicits._
    //
    val begin = System.currentTimeMillis()

    val df = rdd.toDF()
    //     写入 Mysql
    df.write.mode(saveMode).jdbc(url, table, prop)
    val count = df.count()
    val end = System.currentTimeMillis()

    logger.info(s"time:[$time] write [$count] events use time ${(end - begin) / 1000} S ")
  }
}

