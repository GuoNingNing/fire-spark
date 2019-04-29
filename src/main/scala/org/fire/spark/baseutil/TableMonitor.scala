package org.fire.spark.baseutil

import org.apache.spark.sql.{Row, SparkSession}
import org.fire.spark.Notice.{Ding, EMail, send}

import scala.util.{Success, Try}

/**
  * Created on 2019-03-29.
  * Copyright (c) 2019版权所有.
  * author: shumeng.ren
  */
object TableMonitor {



}


class TableMonitor(spark:SparkSession,
                  day:String
                  ) {

    /**
      * 默认是昨天的日期
      * @param spark
      * @return
      */
    def this(spark:SparkSession) = this(spark,DateUtil.getYesterdayDate())


    var msg_text:String = ""

    def monitorOneImpl(info: Map[String, String]):Long = {

        println(s"info$info")
        val partition = info.getOrElse("partition", "ymd")
        val day = info("day")
        val table = info("table")

        val sql = s"select count(*) as sum from $table where $partition = $day"
        println(s"sql:$sql")

        val sum = spark.sql(sql).head().getAs("sum").toString.toLong

        println(s"day:$day  table:$table   sum:$sum")

        sum
    }


    /**
      * 删除分区sql执行语句
      * @param sql
      * @return
      */
    def deletePartitionsImpl(sql:String) = {

        println(s"DeletePartitionsImpl sql:$sql")
        spark.sql(sql)
    }

    def getTablePartitions(sql:String):List[String] = {

        println(s"GetTablePartitions sql:$sql")
        spark.sql(sql).printSchema()
        spark.sql(sql).show(1000)

        spark.sql(sql)
                .rdd
                .map{case Row(partition) => partition.toString.split("=")(1)}
                .collect()
                .toList
    }

    def deletePartitions(info:Map[String,String]): Unit = {

        try {
            val partition = info.getOrElse("partition", "ymd")
            val day = info("day")
            val table = info("table")
            val before_len = info.getOrElse("before_len", "0").trim.toInt

            if (before_len < 1) return

            val before_day = DateUtil.getCareDateBeforeDate(day, before_len)
            val show_partitions_sql = s"show partitions $table"
            val partitions = getTablePartitions(show_partitions_sql)

            println(s"before_day:$before_day  partitions:${partitions.mkString(",")}")

            partitions
                    .filter(x => x < before_day)
                    .foreach(d => {
                        val delete_sql = s"alter table $table drop if exists partition($partition=$d)"
                        deletePartitionsImpl(delete_sql)
                    })
        } catch {
            case ex:Exception => {
                println(s"DeletePartitions exception:${ex.getStackTrace.map(_.toString).mkString("\n")}")
            }
        }
    }


    /**
      *
      * @param info
      */
    def sendMessage(info:Map[String,String]=Map[String,String]()) = {

        try {
            val conf = spark.sparkContext.getConf
            val dd_url = info.getOrElse("dd_url", conf.get("spark.dingding.url",""))
            val at = info.getOrElse("phone", conf.get("spark.dingding.phone", ""))
            if (this.msg_text.length > 0) {
                send a Ding(dd_url, at, this.msg_text.replace("</p>", "\n"))
                send a EMail(
                    to = conf.get("spark.email.to","") ,
                    subject = s"$day${conf.get("spark.email.subject", "离线数据表异常")}",
                    message = this.msg_text,
                    user = conf.get("spark.email.user",""),
                    password = conf.get("spark.email.password",""),
                    addr = conf.get("spark.email.server",""),
                    subtype = "html"
                )
            }
        } catch {
            case ex:Exception => {
                println(s"send_message exception:${ex.getStackTrace.map(_.toString).mkString("\n")}")
            }
        }

    }

    def unusualDataCheck(info_list:List[Map[String,String]]) = {

        val res_list = info_list.filter(x => {
            val threshold_num = x.getOrElse("threshold", "0").trim.toLong
            x.getOrElse("sum", "0").trim.toLong <= threshold_num
        })
                .map(x => {
                    s"table:${x.getOrElse("table", "")}  count:${x.getOrElse("sum", "0").toString}"
                })

        if (res_list.nonEmpty) {
            this.msg_text =
                    s"""
                       |name:${info_list.head("day")}${spark.sparkContext.getConf.get("spark.email.subject", "离线数据表异常")}</p>
                       |${res_list.mkString("</p>")}
                """.stripMargin
        }
    }

    /**
      *
      *
      * @param table_list
      */
    def monitorTableList(table_list:List[Map[String,String]]) = {

        // 清空消息
        msg_text = ""

        // 如果item中带有日期，则以带入的日期为准
        val check_list = table_list.map(item => {

            val temp_item = Map(
                "day" -> day
            ) ++ item
            val sum = Try {
                monitorOneImpl(temp_item)
            } match {
                case Success(s:Long) => s
                case _ => 0L
            }
            temp_item ++ Map("sum" -> sum.toString)
        })
        unusualDataCheck(check_list)
        sendMessage()
    }


    /**
      *
      * @param table
      * @param partition
      */
    def monitorOneTable(table:String, partition:String="ymd") = {

        val item = Map(
            "table" -> table,
            "partition" -> partition
        )
        monitorTableList(List(item))
        sendMessage()
    }
}
