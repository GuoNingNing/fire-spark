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


class TableMonitor(val spark:SparkSession, var day:String) {

    /**
      * 默认是昨天的日期
      * @param spark
      * @return
      */
    def this(spark:SparkSession) = this(spark,DateUtil.getYesterdayDate())

    /**
      * 更新日期
      * @param d
      * @return
      */
    def setDay(d:String):TableMonitor = {
        day = d
        this
    }

    // 文本消息
    var msgText:String = ""

    /**
      *
      * @param info
      * @return
      */
    def monitorOneImpl(info: Map[String, String]):Long = {

        println(s"info$info")
        val partition = info.getOrElse("partition", "ymd")
        val day = info("day")
        val table = info("table")

        val sql = partition match {
            case "no" => s"select count(*) as sum from $table"
            case _ => s"select count(*) as sum from $table where $partition = $day"
        }

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
    def sqlExecImpl(sql:String) = {

        println(s"deletePartitionsImpl sql:$sql")
        spark.sql(sql)
    }

    /**
      * 获取当前表的所有分区
      * @param sql
      * @return
      */
    def getTablePartitions(sql:String):List[String] = {

        println(s"getTablePartitions sql:$sql")
        spark.sql(sql).printSchema()
        spark.sql(sql).show(1000)

        spark.sql(sql)
                .rdd
                .map{case Row(partition) => partition.toString.split("=")(1)}
                .collect()
                .toList
    }

    /**
      * 删除分区操作
      * @param info
      */
    def deletePartitions(info:Map[String,String]): Unit = {

        try {
            val partition = info.getOrElse("partition", "ymd")
            val day = info("day")
            val table = info("table")
            val beforeLen = info.getOrElse("before_len", "0").trim.toInt

            if (beforeLen < 1) return

            val beforeDay = DateUtil.getCareDateBeforeDate(day, beforeLen)
            val showPartitionsSql = s"show partitions $table"
            val partitions = getTablePartitions(showPartitionsSql)

            println(s"beforeDay:$beforeDay  partitions:${partitions.mkString(",")}")

            partitions
                    .filter(x => x < beforeDay)
                    .foreach(d => {
                        val deleteSql = s"alter table $table drop if exists partition($partition=$d)"
                        sqlExecImpl(deleteSql)
                    })
        } catch {
            case ex:Exception => {
                println(s"DeletePartitions exception:${ex.getStackTrace.map(_.toString).mkString("\n")}")
            }
        }
    }


    /**
      * spark.dingding.url
      * spark.dingding.phone
      * spark.email.subject
      * spark.email.to
      * spark.email.password
      * spark.email.server
      * @param info
      */
    def sendMessage(info:Map[String,String]=Map[String,String]()) = {

        try {
            val conf = spark.sparkContext.getConf
            val ddUrl = info.getOrElse("dd_url", conf.get("spark.dingding.url",""))
            val at = info.getOrElse("phone", conf.get("spark.dingding.phone", ""))
            println(
                s"""
                  |dd_url:$ddUrl
                  |at:$at
                  |msg_text:${this.msgText}
                """.stripMargin)

            if (this.msgText.length > 0) {
                send a Ding(ddUrl, at, this.msgText.replace("</p>", "\n"))
                send a EMail(
                    to = conf.get("spark.email.to","") ,
                    subject = s"$day${conf.get("spark.email.subject", "离线数据表异常")}",
                    message = this.msgText,
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

    def unusualDataCheck(infoList:List[Map[String,String]]) = {

        val resList = infoList.filter(x => {
            val thresholdNum = x.getOrElse("threshold", "0").trim.toLong
            x.getOrElse("sum", "0").trim.toLong <= thresholdNum
        })
        .map(x => {
            s"table:${x.getOrElse("table", "")}  count:${x.getOrElse("sum", "0").toString}"
        })

        if (resList.nonEmpty) {
            this.msgText =
                    s"""
                       |name:${infoList.head("day")}${spark.sparkContext.getConf.get("spark.email.subject", "离线数据表异常")}</p>
                       |${resList.mkString("</p>")}
                """.stripMargin
        }
    }

    /**
      *
      *
      * @param tableList
      */
    def monitorTableList(tableList:List[Map[String,String]]): TableMonitor = {

        // 清空消息
        this.msgText = ""

        // 如果item中带有日期，则以带入的日期为准
        val checkList = tableList.map(item => {

            val tempItem = Map(
                "day" -> day
            ) ++ item
            val sum = Try {
                monitorOneImpl(tempItem)
            } match {
                case Success(s:Long) => s
                case _ => 0L
            }
            tempItem ++ Map("sum" -> sum.toString)
        })
        unusualDataCheck(checkList)
        sendMessage()
        this
    }


    /**
      *
      * @param table
      * @param partition
      */
    def monitorOneTable(table:String, partition:String="ymd"): TableMonitor = {

        val item = Map(
            "table" -> table,
            "partition" -> partition
        )
        monitorTableList(List(item))
    }
}
