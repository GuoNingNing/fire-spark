package org.fire.spark.baseutil

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Locale}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Success, Try}

/**
  * Created on 2019-03-29.
  * Copyright (c) 2019 版权所有.
  * author: shumeng.ren
  */
object DateUtil {
    val DATEDATEFORMAT = "yyyyMMdd"
    val DATEDATEFORMAT1 = "yyyy-MM-dd HH:mm:ss"
    val ONEDAY = 1000 * 60 * 60 * 24


    /**
      * 获取当天的日期
      * @return 20161010
      */
    def getTodayDate(): String = {

        val dateFormat: SimpleDateFormat = new SimpleDateFormat(DATEDATEFORMAT)
        dateFormat.format(new Date())
    }

    /**
      * 获取当前的时间戳
      * @return
      */
    def getNowTime(): Long = {
        new Date().getTime
    }

    /**
      * 获取当前的时间戳
      * @return
      */
    def getNowTimePretty(): String = {
        new SimpleDateFormat(DATEDATEFORMAT1).format(new Date().getTime)
    }

    /**
      *
      * @param src  = "2016-08-21T23:53:12-04:00"
      * @return
      */
    def convertDateToStamp(src:String, format:String = DATEDATEFORMAT1): Long = {
        val v = src.replace("T", "")
        val loc = new Locale("en")
        new SimpleDateFormat(format, loc).parse(v.substring(0, v.lastIndexOf('-'))).getTime
    }

    /**
      * 将指定日期转换为时间戳
      * @param src
      * @param format
      * @return
      */
    def convertDateToStampNew(src:String, format:String = DATEDATEFORMAT1): Long = {
        val loc = new Locale("en")
        new SimpleDateFormat(format, loc).parse(src).getTime
    }

    /**
      * 获取昨天的日期
      * @return
      */
    def getYesterdayDate(): String = {

        val dateFormat: SimpleDateFormat = new SimpleDateFormat(DATEDATEFORMAT)
        val cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, -1)
        dateFormat.format(cal.getTime())
    }

    /**
      * 获取ming tian的日期
      * @return
      */
    def getTomorrowDate(): String = {

        val dateFormat: SimpleDateFormat = new SimpleDateFormat(DATEDATEFORMAT)
        val cal: Calendar = Calendar.getInstance()
        cal.add(Calendar.DATE, 1)
        dateFormat.format(cal.getTime)
    }

    /**
      * 获取从本日起的last_day天的日期
      * @param cur
      * @param last_day 持续时间
      */
    def getCareDateBeforeDateList(cur: String, last_day: Int) = {

        val res = ArrayBuffer[String]()
        for (d <- 0 until last_day) {
            res += getCareDateBeforeDate(cur, d)
        }
        res.toArray
    }

    /**
      * 获取和目标日期间隔的日期
      * @param cur
      * @param gapDay
      * @return
      */
    def getCareDateBeforeDate(cur: String, gapDay: Int) = {

        val loc = new Locale("en")
        val dateFormat = new SimpleDateFormat(DATEDATEFORMAT, loc)
        val dstDay = dateFormat.parse(cur)
        val cal: Calendar = Calendar.getInstance()
        cal.setTimeInMillis(dstDay.getTime())
        cal.add(Calendar.DATE, -1 * gapDay)
        dateFormat.format(cal.getTime())
    }


    def getCareDurDate(left:String, right: String): Array[String] = {
        val left_stamp = convertDateToStampNew(left, DATEDATEFORMAT)
        val right_stamp = convertDateToStampNew(right, DATEDATEFORMAT)

        val dur = (right_stamp - left_stamp) / ONEDAY + 1

        getCareDateBeforeDateList(right, dur.toInt)
    }

    // /user/hive/warehouse/hallone.db/als_user_anchor_rate
    // hallone.als_user_anchor_rate

    /**
      * 获取HIVE的基本路径
      * @param table
      * @param src_path
      * @return
      */
    def getBaseHivePath(table:String, src_path:String = "/user/hive/warehouse/"): String = {
        val sp = if (src_path.endsWith("/")) src_path else src_path + "/"
        sp + table.replace(".", ".db/")
    }


    def getCareDateTimestamp(cur:String, dateFormat:String=DATEDATEFORMAT): Long = {

        val loc = new Locale("en")
        val dateFormat = new SimpleDateFormat(DATEDATEFORMAT, loc)
        val dstTime = dateFormat.parse(cur)
        dstTime.getTime
    }


    /**
      * split时，一些字符需要转义
      * @param str
      * @return
      */
    def sepEscape(str:String):String = {
        str match {
            case "|" => "\\|"
            case "^" => "\\^"
            case _ => str
        }
    }


    /**
      * 打印時間
      * @param v
      * @param unit
      */
    def printTime(v:Long, unit:String="ms"):Unit = {
        if (v <= 0) return
        val a = 1000L
        val b = 60L
        val c = 24L
        val sep = ""
        unit match {
            case "ms" => printTime(v / a, "s"); print(s"${v % a}ms")
            case "s" => printTime(v / b, "m"); print(s"${v % b}s$sep")
            case "m" => printTime(v / b, "h"); print(s"${v % b}m$sep")
            case "h" => printTime(v / c, "d"); print(s"${v % c}h$sep")
            case "d" =>  print(s"${v}d$sep")
            case _ => print("param error")
        }
    }

    /**
      * 数据格式化
      * @param d
      * @param len
      * @return
      */
    def dataFormat(d:Any, len:Int=5):String = {

        val str_data = d match {
            case s:Float => s"%.${len}f".format(s)
            case s:Double => s"%.${len}f".format(s)
            case s:Int => s"%.${len}f".format(s)
            case s => {
                Try {
                    s.toString.trim.toDouble
                } match {
                    case Success(v:Double) => s"%.${len}f".format(v)
                    case _ => s.toString.trim
                }
            }
        }
        str_data
                .replaceAll("0+?$", "")
                .replaceAll("[.]$", "")
    }

}
