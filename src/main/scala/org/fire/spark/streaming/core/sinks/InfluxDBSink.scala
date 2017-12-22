package org.fire.spark.streaming.core.sinks

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{StreamingContext, Time}
import scalaj.http._

import scala.reflect.ClassTag

/**
  * Created by cloud on 17/12/22.
  *
  * 暂不支持checkpoint模式
  */
class InfluxDBSink[T : ClassTag](@transient
                                 override val ssc : StreamingContext)
  extends Sink[T]{

  private val host = sparkConf.get("spark.sink.influxDB.host")
  private val port = sparkConf.get("spark.sink.influxDB.port","8086")
  private val db = sparkConf.get("spark.sink.influxDB.db","influx")

  def output(rdd : RDD[T],time : Time = Time(System.currentTimeMillis())): Unit = {
    rdd.foreach(d => {
      val (postData,ip,pt,dbName) = d match {
        case t : String => (t,host,port,db)
        case (k : String,v : String) =>
          val info = k.split(",")
          info.size match {
            case 1 => (v,info(0),port,db)
            case 2 => (v,info(0),info(1),db)
            case 3 => (v,info(0),info(1),info(2))
            case _ => (v,host,port,db)
          }
        case (h : String,p : String,d : String,v : String) => (v,h,p,d)
        case _ => (d.toString,host,port,db)
      }
      val res: HttpResponse[String] = Http(s"http://$ip:$pt/write?db=$dbName").postData(postData).asString
      res.code match {
        case d if d >= 200 && d < 300 =>
          logger.info(s"Write influxDB successful. ${res.code}")
        case d if d >= 400 =>
          logger.error(s"Write influxDB failed. $res")
        case _ =>
          logger.warn(s"Write influxDB unknown error. $res")
      }
    })
  }

}

object InfluxDBSink {
  def apply(ssc : StreamingContext) = new InfluxDBSink[String](ssc)
}
