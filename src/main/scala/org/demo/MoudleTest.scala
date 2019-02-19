package org.demo

import org.fire.spark.Notice.{Ding, EMail, send}


/**
  * Created on 2018-07-14.
  * Copyright (c) 2018, ingkee版权所有.
  * Author: shumeng.ren
  */
object MoudleTest {

    def main(args: Array[String]): Unit = {

        send a EMail(
            to = "rensm@inke.cn,rensm@inke.cn",
            subject = "mytest",
            message = "xxxxx",
            user = "rec_basic_monitor@inke.cn",
            password = "W7]7ll6jdDl5eM7",
            addr = "mail.inke.cn"
        )

        send a Ding(
            api = "https://oapi.dingtalk.com/robot/send?access_token=afcf4985be92f11796b314d4e0290c76269ecb99d7566e559ce13a68c637894c",
            to = "177013087",
            message = "test"
        )
    }
}
