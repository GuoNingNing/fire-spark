package org.apache.spark.deploy.yarn

import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import org.apache.hadoop.yarn.api.records.{ApplicationId, FinalApplicationStatus, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException
import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.{SparkHadoopUtil, SparkSubmit, SparkSubmitArguments}
import org.apache.spark.deploy.yarn.MonitorResponseResult.MonitorResponseResult
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.util.{IntParam, ThreadUtils, Utils}

import scala.collection.mutable.HashMap
import scala.annotation.tailrec
import scala.collection.JavaConversions._

/**
  * Created by cloud on 18/1/17.
  */
private[spark] class YarnApplicationMonitorServer(
    override val rpcEnv: RpcEnv,
    val conf : SparkConf) extends ThreadSafeRpcEndpoint with Logging{

  private val appIdMap = new ConcurrentHashMap[ApplicationId,StartCommand]()
  private var quit : Boolean = false
  private val yarnClient : YarnClient = YarnClient.createYarnClient()
  private val checkAppReportThread = ThreadUtils.newDaemonSingleThreadScheduledExecutor("yarn-application-check-report")
  private var checkAppReportFuture : ScheduledFuture[_] = _

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case MonitorAppRequest(appId,command) =>
      appIdMap += appId -> command
      context.reply(MonitorAppResponse("success",MonitorResponseResult.SUCCESS))

    case KillMonitorAppRequest(appId) =>
      appIdMap -= appId
      yarnClient.killApplication(appId)
      context.reply(MonitorAppResponse("success",MonitorResponseResult.SUCCESS))

    case CancelMonitorAppRequest(appId) =>
      appIdMap -= appId
      context.reply(MonitorAppResponse("success",MonitorResponseResult.SUCCESS))

    case _ =>
      context.reply(MonitorAppResponse("success",MonitorResponseResult.UNKNOWN))

  }

  override def onStop(): Unit = {
    quit = true
    if(null != checkAppReportFuture){
      checkAppReportFuture.cancel(true)
    }
    checkAppReportThread.shutdownNow()
    yarnClient.stop()
  }

  private def startApp(appId : ApplicationId): Unit = {
    val command = appIdMap(appId)
    ThreadUtils.runInNewThread(s"app-monitor-$appId-submit",isDaemon = false) {
      SparkSubmit.submit(new SparkSubmitArguments(command.command.split("\u0001")))
    }
    logInfo(s"Start APP ${command.command}")
    appIdMap -= appId
  }

  override def onStart(): Unit = {
    val yarnConfiguration = new YarnConfiguration(SparkHadoopUtil.get.newConfiguration(conf))
    yarnClient.init(yarnConfiguration)
    yarnClient.start()
    checkAppReportFuture = checkAppReportThread.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        try{
          checkApplication()
        }catch {
          case e : Exception => logError("checkApplication exception. ",e)
        }
      }
    },1000L,1000L,TimeUnit.MILLISECONDS)
  }

  /**
    * 检查 App 的运行状态
    */
  private def checkApplication(): Unit = {
    appIdMap.foreach { case (appId, _) =>
      try {
        val appReport = yarnClient.getApplicationReport(appId)
        appReport.getYarnApplicationState match {
          case YarnApplicationState.FINISHED =>
            appReport.getFinalApplicationStatus match {
              case FinalApplicationStatus.KILLED => startApp(appId)
              case FinalApplicationStatus.FAILED => startApp(appId)
              case _ =>
            }
          case YarnApplicationState.KILLED => startApp(appId)
          case YarnApplicationState.FAILED => startApp(appId)
        }
      } catch {
        case e: ApplicationNotFoundException => startApp(appId)
        case e: Exception => logError("getApplicationReport failed. ", e)
      }
      try {
        Thread.sleep(1000L)
      } catch {
        case e: Exception => logError("Thread.sleep(1000L) error : ", e)
      }
    }
  }


}

object YarnApplicationMonitorServer extends Logging{
  private val systemName = "application-monitor"
  private val endpointSerName = "app-monitor-ser"
  val MONITOR_HOST = "spark.application.monitor.server.host"
  val MONITOR_PORT = "spark.application.monitor.server.port"

  def main(args: Array[String]): Unit = {
    Utils.initDaemon(log)
    val argument = new YarnApplicationMonitorServerArguments(args)
    val sparkConf = new SparkConf()
    val securityManager = new SecurityManager(sparkConf)

    val rpcEnv = RpcEnv.create(systemName,argument.host,argument.port,sparkConf,securityManager)
    val yarnApplicationMonitorServer = new YarnApplicationMonitorServer(rpcEnv,sparkConf)
    rpcEnv.setupEndpoint(endpointSerName,yarnApplicationMonitorServer)
    rpcEnv.awaitTermination()
  }

  def createYarnAppMonitorCli(sparkConf: SparkConf) : RpcEndpointRef = {
    val host = sparkConf.get(MONITOR_HOST,Utils.localHostName())
    val port = sparkConf.getInt(MONITOR_PORT,23456)
    createYarnAppMonitorCli(sparkConf, host, port)
  }

  def createYarnAppMonitorCli(
                               sparkConf: SparkConf,
                               host : String,
                               port : Int) : RpcEndpointRef = {
    val securityManager = new SecurityManager(sparkConf)
    val rpcEnv = RpcEnv.create(systemName,host,host,port,sparkConf,securityManager,true)
    rpcEnv.setupEndpointRef(RpcAddress(host,port),endpointSerName)
  }

}

private class YarnApplicationMonitorServerArguments(
    val args : Array[String]
                                                   ) extends Logging {


  var host = Utils.localHostName()
  var port = 23456
  var propertiesFile : String = _
  private lazy val properties : HashMap[String,String] = {
    val p = new HashMap[String,String]()
    Option(propertiesFile).foreach(f => {
      val ps = Utils.getPropertiesFromFile(f)
      ps.foreach {case (k,v) => p(k)=v}
    })
    p
  }


  parse(args.toList)
  propertiesFile = Option(propertiesFile).getOrElse(Utils.getDefaultPropertiesFile())
  properties.foreach {case (k,v) => System.setProperty(k,v)}
  if(System.getenv.contains(YarnApplicationMonitorServer.MONITOR_HOST)){
    host=System.getProperty(YarnApplicationMonitorServer.MONITOR_HOST)
  }
  if(System.getenv.contains(YarnApplicationMonitorServer.MONITOR_PORT)){
    port=System.getProperty(YarnApplicationMonitorServer.MONITOR_PORT,"23456").toInt
  }


  @tailrec
  private def parse(args : List[String]): Unit = args match {
    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value,"Please use hostname " + value)
      host = value
      parse(tail)

    case ("--ip" | "-i") :: value :: tail =>
      Utils.checkHost(value,"ip no longer supported, please use hostname " + value)
      host=value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port=value
      parse(tail)

    case ("--properties-file") :: value :: tail =>
      propertiesFile=value
      parse(tail)

    case ("--help" | "-H") :: tail => printUsageAndExit(0)
    case Nil => //No-op
    case _ => printUsageAndExit(1)

  }

  private def printUsageAndExit(exitCode : Int): Unit = {
    System.err.println(
      "Usage: YarnApplicationMonitorServer [options]\n" +
        "\n" +
        "Options:\n" +
        "  -i HOST, --ip HOST     Hostname to listen on (deprecated, please use --host or -h) \n" +
        "  -h HOST, --host HOST   Hostname to listen on\n" +
        "  -p PORT, --port PORT   Port to listen on (default: 7077)\n" +
        "  --properties-file FILE Path to a custom Spark properties file.\n" +
        "                         Default is conf/spark-defaults.conf.\n" +
        "  -H, --help             Print help info.")
    System.exit(exitCode)
  }

}

private[spark] case class MonitorAppRequest(appId : ApplicationId,command : StartCommand)
private[spark] case class StartCommand(command : String)
private[spark] case class KillMonitorAppRequest(appId : ApplicationId)
private[spark] case class CancelMonitorAppRequest(appId : ApplicationId)
private[spark] case class MonitorAppResponse(info : String,state : MonitorResponseResult)
private[spark] object MonitorResponseResult extends Enumeration {
  type MonitorResponseResult = Value
  val SUCCESS,FAILURE,UNKNOWN=Value
}
