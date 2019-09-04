package org.demo

import org.apache.spark.streaming.{Milliseconds, Seconds}

/**
  * Created on 2019-05-27.
  * Copyright (c) 2019, ingkee版权所有.
  * author: shumeng.ren
  */
object myTest {


    def testBatch(): Unit = {
        val slide = .5
        val batchTime = if (slide >= 1.0) {
            Seconds(slide.toInt)
        } else {
            Milliseconds((slide * 1000).toInt)
        }
        println(batchTime)
    }

    def main(args: Array[String]): Unit = {
        //        send a Ding("https://oapi.dingtalk.com/robot/send?access_token=afcf4985be92f11796b314d4e0290c76269ecb99d7566e559ce13a68c637894c",
        //            "17701308753",
        //            "cc --> info 没数据了")

        testBatch()
    }
}
