package org.apache.spark

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.util.Utils

import scala.concurrent.duration.FiniteDuration

/**
  * Created by cloud on 18/1/22.
  */
class RpcDemo(
             override val rpcEnv: RpcEnv
             ) extends ThreadSafeRpcEndpoint with Logging{

  override def onStart(): Unit = {
    logInfo("start self server......")
  }

  override def receive: PartialFunction[Any, Unit] = {
    case DemoSendReq(info) => logInfo(s"client send message $info")
  }

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case DemoReq(info) =>
      context.reply(DemoRes(info.reverse,200))
    case _ =>
      context.reply(DemoRes("Not found Request",404))
  }

  override def onStop(): Unit = {
    logInfo("stop self server......")
  }

}

object RpcDemo{
  val SYSTEM_NAME = "rpc-demo"
  val ENDPOINT_NAME = "rpc-demo-ser"
  var rpcDemoRef : RpcEndpointRef = _

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val securityManager = new SecurityManager(sparkConf)
    val host = Utils.localHostName()
    val port = 23456 //port > 2000 && port < 65535
    val rpcEnv = RpcEnv.create(SYSTEM_NAME,host,port,sparkConf,securityManager)

    rpcDemoRef = rpcEnv.setupEndpoint(ENDPOINT_NAME,new RpcDemo(rpcEnv))
    localMsg("init rpc demo request")
    rpcEnv.awaitTermination()
  }

  def localMsg(msg : String): Unit = {
    if(rpcDemoRef != null){
      rpcDemoRef.send(DemoSendReq(msg))
    }
  }

  def createRpcDemoRef(sparkConf: SparkConf,host : String,port : Int) : RpcEndpointRef = {
    val securityManager = new SecurityManager(sparkConf)
    val rpcEnv = RpcEnv.create(SYSTEM_NAME,host,host,port,sparkConf,securityManager,true)
    rpcEnv.setupEndpointRef(RpcAddress(host,port),ENDPOINT_NAME)
  }
}

object RpcDemoCli{
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val timeoutString = "spark.rpc.demo.timeout"
    val timeout = sparkConf.getLong(timeoutString,30000L)
    val host = Utils.localHostName() //server endpoint host
    val port = 23456 //server endpoint port
    val rpcDemoRef = RpcDemo.createRpcDemoRef(sparkConf,host,port)

    rpcDemoRef.send(DemoSendReq("this is request not response"))

    val rpcTimeout = new RpcTimeout(FiniteDuration(timeout,TimeUnit.MILLISECONDS),timeoutString)
    val res = rpcDemoRef.askSync[DemoRes](DemoReq("returnResponse"),rpcTimeout)
    println(s"code : ${res.code}, info : ${res.info}")
  }
}

case class DemoReq(info : String)
case class DemoSendReq(info : String)
case class DemoRes(info : String,code : Int)
