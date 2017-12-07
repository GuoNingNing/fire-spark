/**
  * Created by guoning on 2017/6/6.
  *
  * 发送 DingDing
  *
  *
  */
package object notice {

  case class Ding(api: String, to: String, message: String)

  object send {

    def a(ding: Ding): Unit = {

      import scala.sys.process._


      val body =
        s"""
           |{
           |  "msgtype": "text",
           |  "text": {
           |    "content": "${ding.message}"
           |  },
           |  "at": {
           |    "atMobiles": [
           |      "${ding.to}"
           |    ],
           |    "isAtAll": false
           |  }
           |}
        """.stripMargin

      val cmd = Seq("curl", "-s", "-L", "-X", "POST", "-H", "Content-Type: application/json", "-d " + body, ding.api)

      val result = cmd !!

      println(s"result $result")
    }
  }

}
