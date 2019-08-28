package org.fire.spark

import javax.mail.internet.InternetAddress
import javax.mail.{Authenticator, PasswordAuthentication}

import com.solarmosaic.client.mail.EnvelopeWrappers
import com.solarmosaic.client.mail.content.ContentType.MultipartTypes
import org.apache.spark.SparkConf

/**
  * Created by guoning on 2017/6/6.
  *
  * 发送 DingDing
  *
  *
  */
package object Notice {

    import org.fire.spark.streaming.core.kit.Utils

    case class Ding(api: String, to: String, message: String)

    case class EMail(to: String,
                     subject: String,
                     message: String,
                     user: String,
                     password: String,
                     addr: String = "",
                     port: Int = 587,
                     tls: Boolean = false,
                     subtype: String = "text"
                    ) extends Authenticator {
        override def getPasswordAuthentication(): PasswordAuthentication = {
            new PasswordAuthentication(user, password)
        }
    }

    /**
      * 发送通知消息
      *
      * @param conf
      * @param info
      */
    def sendMessage(conf: SparkConf,
                    info: Map[String, String] = Map[String, String]()): Unit = {

        val dd_url = info.getOrElse("dd_url", conf.get("spark.dingding.url", ""))
        val at = info.getOrElse("phone", conf.get("spark.dingding.phone", ""))

        try {

            val message = info("message").toString

            println(
                s"""
                   |dd_url:$dd_url
                   |at:$at
                   |message:$message
                """.stripMargin)

            if (message.length > 0) {
                send a Ding(dd_url, at, message.replace("</p>", "\n"))
                send a EMail(
                    to = conf.get("spark.email.to", ""),
                    subject = s"${conf.get("spark.email.subject", "离线数据表异常")}",
                    message = message,
                    user = conf.get("spark.email.user", ""),
                    password = conf.get("spark.email.password", ""),
                    addr = conf.get("spark.email.server", ""),
                    subtype = "html"
                )
            }
        } catch {
            case ex: Exception => {

                val errMsg =
                    s"""
                       | info:$info
                       | message:${ex.getMessage}
                       | stack:${ex.getStackTrace.map(_.toString).mkString("</p>")}
                    """.stripMargin
                println(s"send_message exception:${errMsg}")
                send a Ding(dd_url, at, errMsg.replace("</p>", "\n"))
            }
        }

    }

    object send extends EnvelopeWrappers {

        def a(ding: Ding): Unit = {

            val body =
                s"""
                   |{
                   |  "msgtype": "text",
                   |  "text": {
                   |    "content": "${ding.message}"
                   |  },
                   |  "at": {
                   |    "atMobiles": [
                   |      ${ding.to}
                   |    ],
                   |    "isAtAll": false
                   |  }
                   |}
        """.stripMargin

            val headers = Map("content-type" -> "application/json")
            val (code, res) = Utils.httpPost(ding.api, body, headers)

            println(s"result code : $code , body : $res")
        }


        def a(email: EMail): Unit = {

            import com.solarmosaic.client.mail._
            import com.solarmosaic.client.mail.configuration._
            import com.solarmosaic.client.mail.content._


            val config = SmtpConfiguration(host = email.addr,
                port = email.port,
                tls = email.tls,
                debug = false,
                authenticator = Some(email)
            )
            val mailer = Mailer(config)
            val content = Multipart(
                parts = Seq(Text(email.subtype), Html(s"<p>${email.message}</p>")),
                subType = MultipartTypes.alternative
            )

            val envelope = Envelope(
                from = email.user,
                to = email.to.split(",").toSeq.map(x => new InternetAddress(x)),
                subject = email.subject,
                content = content
            )
            mailer.send(envelope)
        }
    }

}