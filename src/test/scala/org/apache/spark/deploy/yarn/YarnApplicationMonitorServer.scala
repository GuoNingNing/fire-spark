package org.apache.spark.deploy.yarn

import java.io.{File, PrintWriter}
import java.util.concurrent.{ConcurrentHashMap, ScheduledFuture, TimeUnit}

import org.apache.hadoop.yarn.api.records.{ApplicationId, FinalApplicationStatus, YarnApplicationState}
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException
import org.apache.spark.deploy.yarn.MonitorResponseResult.MonitorResponseResult
import org.apache.spark.deploy.{SparkHadoopUtil, SparkSubmit, SparkSubmitArguments}
import org.apache.spark.internal.Logging
import org.apache.spark.rpc._
import org.apache.spark.util.{IntParam, ThreadUtils, Utils}
import org.apache.spark.{SecurityManager, SparkConf}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.collection.mutable.{HashMap => MHashMap}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source
import scala.util.control.NonFatal
import scala.util.{Failure, Success}

/**
  * Created by cloud on 18/1/17.
  *
  * 这是通过spark-submit来提交任务的监控服务
  * 对应的spark-submit调整代码在test中
  * 如若使用编译代码之后把对应的class替换到spark对应的jar中既可以使用
  * 变更代码只增加了提交监控的函数,其它任何功能均为做变更
  *
  */
private[spark] class YarnApplicationMonitorServer(
    override val rpcEnv: RpcEnv,
    val conf : SparkConf) extends ThreadSafeRpcEndpoint with Logging{

  private val appIdMap = new ConcurrentHashMap[String,AppStatus]()
  private val attemptMap = new ConcurrentHashMap[String,Int]()
  private val scheduledMap = new ConcurrentHashMap[String,Long]()
  private val jobNameMap = new ConcurrentHashMap[String,AppStatus]()
  private val waitConstantTime = 3600*24*100
  private var quit : Boolean = false
  private val yarnClient : YarnClient = YarnClient.createYarnClient()
  private val checkAppReportThread = ThreadUtils.newDaemonSingleThreadScheduledExecutor("yarn-application-check-report")
  private var checkAppReportFuture : ScheduledFuture[_] = _

  private val monitorInterval = conf.getLong(YarnApplicationMonitorServer.MONITOR_INTERVAL,5000L)
  private val attemptNumber = conf.getInt(YarnApplicationMonitorServer.ATTEMPT_NUMBER,3)
  private val checkpointFile = conf.get(YarnApplicationMonitorServer.CHECKPOINT_FILE,"yarnApplicationMonitorSer.cp")
  private val yarnConfiguration = new YarnConfiguration(SparkHadoopUtil.get.newConfiguration(conf))

  private val CIFS = YarnApplicationMonitorServer.COMMAND_IFS
  private val PIFS = YarnApplicationMonitorServer.PARAM_IFS

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case MonitorAppRequest(appId,command) =>
      val attempt = attemptMap.getOrElse(command.command,0)
      if(attempt < attemptNumber) {
        attemptMap += command.command -> (attempt + 1)
        appIdMap += appId -> AppStatus(0, command)
      }
      context.reply(MonitorAppResponse(s"register $appId monitor success",MonitorResponseResult.SUCCESS))

    case KillMonitorAppRequest(appId) =>
      try {
        yarnClient.killApplication(YarnApplicationMonitorServer.appIdFromString(appId))
        context.reply(MonitorAppResponse(s"kill application $appId success", MonitorResponseResult.SUCCESS))
      }catch {
        case NonFatal(e) =>
          logError(s"kill application $appId failed. ",e)
          context.reply(MonitorAppResponse(e.getMessage, MonitorResponseResult.FAILURE))
      }

    case CancelMonitorAppRequest(appId) =>
      try{
        attemptMap -= appIdMap.getOrElse(appId,AppStatus(0,StartCommand(""))).command.command
        appIdMap -= appId
        yarnClient.killApplication(YarnApplicationMonitorServer.appIdFromString(appId))
        context.reply(MonitorAppResponse(s"$appId monitor cancel success.",MonitorResponseResult.SUCCESS))
      }catch {
        case NonFatal(e) =>
          if(jobNameMap.contains(appId)){
            jobNameMap -= appId
            scheduledMap -= appId
            context.reply(MonitorAppResponse(s"$appId scheduled cancel success.",MonitorResponseResult.SUCCESS))
          }else {
            logError(s"cancel application $appId failed. ", e)
            context.reply(MonitorAppResponse(e.getMessage, MonitorResponseResult.FAILURE))
          }
      }

    case GetMonitorAppRequest(appId) =>
      if(appIdMap.containsKey(appId) || jobNameMap.containsKey(appId)) {
        val appStatus = appIdMap.getOrElse(appId,jobNameMap.getOrElse(appId,AppStatus(0,StartCommand(""))))
        val cmdInfo = appStatus.command.command.split(CIFS).mkString("[\"", "\",\"", "\"]")
        val info = s"""$appId : {"status" : ${appStatus.status},"cmd" : $cmdInfo}"""
        context.reply(MonitorAppResponse(info, MonitorResponseResult.SUCCESS))
      } else {
        val appIdInfo = appIdMap.keys().mkString("[\"", "\",\"", "\"]")
        val jobNameInfo = jobNameMap.keys().mkString("[\"", "\",\"", "\"]")
        val info = s"""{"monitorApp":$appIdInfo,"scheduledJob":$jobNameInfo}"""
        context.reply(MonitorAppResponse(info, MonitorResponseResult.SUCCESS))
      }

    case ScheduledAppRequest(jobName,scheduled,command) =>
      val cmd = command.command.split(CIFS).filter(_ != "--monitor").mkString(CIFS)
      jobNameMap += jobName -> AppStatus(scheduled.toInt,StartCommand(cmd))
      scheduledMap += jobName -> scheduled
      runScheduledApp(jobName)
      context.reply(MonitorAppResponse(s"$jobName scheduler set success", MonitorResponseResult.SUCCESS))

    case _ =>
      context.reply(MonitorAppResponse("unknown request",MonitorResponseResult.UNKNOWN))

  }

  override def onStop(): Unit = {
    quit = true
    if(null != checkAppReportFuture){
      checkAppReportFuture.cancel(true)
    }
    checkAppReportThread.shutdownNow()
    yarnClient.stop()
  }

  /**
    * 这个方法逻辑有待进一步处理,过早的清除appIdMap中对应的appId可能导致任务重启失败后不会再次重启
    * 但是过晚的清理appIdMap中的appId可能导致第二次被检测到任务失败,而被重复启动
    *
    * spark.yarn.submit.waitAppCompletion=false提交完任务后就退出
    */
  private def startApp(appId : String): Unit = {
    try{
      if(appIdMap.containsKey(appId)) {
        val appStatus = appIdMap(appId)
        val cmd = appStatus.command.command
        appIdMap += appId -> AppStatus(appStatus.status + 1,StartCommand(cmd))
        val res = runInNewThread(s"app-monitor-$appId-submit") {
          SparkSubmit.submit(new SparkSubmitArguments(cmd.split(CIFS)))
        }
        res.onComplete {
          case Success(s) =>
            appIdMap -= appId

          case Failure(e) =>
            logWarning(s"attempts ${appStatus.status} restart app $appId failed. ",e)
        }
      }
    }catch {
      case NonFatal(e) => logError(s"startApp $appId failed. ",e)
    }
  }

  override def onStart(): Unit = {
    yarnClient.init(yarnConfiguration)
    yarnClient.start()
    recoveryCheckPoint()
    checkAppReportFuture = checkAppReportThread.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = {
        try{
          flushCheckPoint()
          checkScheduledApp()
          checkApplication()
        }catch {
          case NonFatal(e) => logError("checkApplication exception. ",e)
        }
      }
    },1000L,monitorInterval,TimeUnit.MILLISECONDS)
  }

  private def checkApplication(): Unit = {
    appIdMap.filter(d => d._2.status == 0).foreach { case (appId, _) =>
      try {
        val appReport = yarnClient.getApplicationReport(YarnApplicationMonitorServer.appIdFromString(appId))
        appReport.getYarnApplicationState match {
          case YarnApplicationState.FINISHED =>
            appReport.getFinalApplicationStatus match {
              case FinalApplicationStatus.KILLED => startApp(appId)
              case FinalApplicationStatus.FAILED => startApp(appId)
              case _ => //No-op
            }
          case YarnApplicationState.KILLED => startApp(appId)
          case YarnApplicationState.FAILED => startApp(appId)
          case _ => //No-op
        }
      } catch {
        case e: ApplicationNotFoundException => startApp(appId)
        case NonFatal(e) => logError("getApplicationReport failed. ", e)
      }
    }
    val activeCmdList = appIdMap.filter(d => d._2.status == 0).map(_._2.command.command).toList
    appIdMap.filter(d => d._2.status != 0).foreach { case (appId, appStatus) =>
      if(activeCmdList.contains(appStatus.command.command)){
        appIdMap -= appId
      }else{
        /**
          * appStatus.status 表明任务submit失败次数,因为submit成功之后status就会被清零
          * attempt 表明任务running次数,因为只有running时才会提交监控attempt次数增加
          * 
          */
        val attempt = attemptMap.getOrElse(appStatus.command.command,0)
        if(appStatus.status < attemptNumber && attempt < attemptNumber){
          startApp(appId)
        }
      }
    }
  }

  private def checkScheduledApp(): Unit = {
    scheduledMap.foreach {
      case (jobName, schedule) =>
        if(schedule < 0){
          runScheduledApp(jobName)
        }else{
          scheduledMap += jobName -> (schedule - monitorInterval/1000)
        }
    }
  }

  private def runScheduledApp(jobName: String): Unit = {
    val res = runInNewThread(jobName){
      SparkSubmit.submit(new SparkSubmitArguments(jobNameMap(jobName).command.command.split(CIFS)))
    }
    scheduledMap += jobName -> (jobNameMap(jobName).status + waitConstantTime)
    res.onComplete {
      case Success(s) =>
        logInfo(s"$jobName scheduled success.")
        scheduledMap += jobName -> (scheduledMap(jobName) - waitConstantTime)

      case Failure(e) =>
        logError(s"$jobName scheduled failed. ",e)
        scheduledMap += jobName -> (scheduledMap(jobName) - waitConstantTime)
    }
  }

  private def runInNewThread[T](threadName: String)(func: => T): Future[T] = {
    Future {
      ThreadUtils.runInNewThread(threadName){
        func
      }
    }
  }

  private def flushCheckPoint(): Unit = {
    try {
      val file = new File(checkpointFile)
      val printWriter = new PrintWriter(file)
      appIdMap.foreach { case (appId, appStatus) =>
        printWriter.write(s"${appId}$PIFS${appStatus.status}$PIFS${appStatus.command.command}\n")
      }
      jobNameMap.foreach { case (jobName, appStatus) =>
        printWriter.write(s"${jobName}$PIFS${appStatus.status}$PIFS${appStatus.command.command}\n")
      }
      printWriter.close()
    } catch {
      case NonFatal(e) => logError("flushCheckPoint failed. ",e)
    }
  }

  private def recoveryCheckPoint(): Unit = {
    try {
      val file = new File(checkpointFile)
      val source = Source.fromFile(file)
      source.getLines().map(_.trim.split(PIFS)).filter(_.length == 3).foreach {
        lines =>
          val status = lines(1).toInt
          val appId = lines(0)
          val appStatus = AppStatus(status,StartCommand(lines(2)))
          logInfo(s"load application $appId success.")
          if(status > 10){
            jobNameMap += appId -> appStatus
            scheduledMap += appId -> status
          }else{
            appIdMap += appId -> appStatus
          }
      }
      source.close()
      logInfo("recoveryCheckPoint success.")
    } catch {
      case NonFatal(e) => logError("recoveryCheckPoint failed. ",e)
    }
  }

}

object YarnApplicationMonitorServer extends Logging{
  private val systemName = "application-monitor"
  private val endpointSerName = "app-monitor-ser"
  val MONITOR_PREFIX = "spark.application.monitor"
  val MONITOR_HOST = s"$MONITOR_PREFIX.server.host"
  val MONITOR_PORT = s"$MONITOR_PREFIX.server.port"
  val MONITOR_INTERVAL = s"$MONITOR_PREFIX.interval.ms"
  val CHECKPOINT_FILE = s"$MONITOR_PREFIX.checkpoint.file"
  val ATTEMPT_NUMBER = s"$MONITOR_PREFIX.attempt.number"
  val COMMAND_IFS = "\u0001"
  val PARAM_IFS = "\u0000"

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

  def createYarnAppMonitorCli(sparkConf: SparkConf,
                              host : String,
                              port : Int) : RpcEndpointRef = {
    val securityManager = new SecurityManager(sparkConf)
    val rpcEnv = RpcEnv.create(systemName,host,host,port,sparkConf,securityManager,true)
    rpcEnv.setupEndpointRef(RpcAddress(host,port),endpointSerName)
  }

  def appIdFromString(appIdStr : String): ApplicationId = {
    val appIdPrefix = "application_"
    if(!appIdStr.startsWith(appIdPrefix)){
      throw new IllegalArgumentException("Invalid ApplicationId: "
        + appIdStr)
    }
    try {
      val pos1 = appIdPrefix.length
      val pos2 = appIdStr.indexOf('_', pos1)
      if (pos2 < 0) {
        throw new IllegalArgumentException("Invalid ApplicationId: "
          + appIdStr)
      }
      val rmId = appIdStr.substring(pos1, pos2).toLong
      val appId = appIdStr.substring(pos2 + 1).toInt
      ApplicationId.newInstance(rmId, appId)
    } catch {
      case e : NumberFormatException =>
        throw new IllegalArgumentException("Invalid ApplicationId: "
        + appIdStr, e)
    }
  }

}

private class YarnApplicationMonitorServerArguments(val args : Array[String]) extends Logging {

  var host = Utils.localHostName()
  var port = 23456
  var propertiesFile : String = _
  private lazy val properties : MHashMap[String,String] = {
    val p = new MHashMap[String,String]()
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

object YarnApplicationMonitorClient extends Logging{
  private var host = Utils.localHostName()
  private var port = 23456
  private var cmd : String = _
  private var param : String = _
  private val IFS = YarnApplicationMonitorServer.PARAM_IFS
  private val LIST = "list"
  private val CANCEL = "cancel"
  private val KILL = "kill"
  private val REGISTER = "register"
  private val SCHEDULED = "scheduled"

  def main(args: Array[String]): Unit = {
    parse(args.toList)
    cmd = Option(cmd).getOrElse("list")
    param = Option(param).getOrElse("")

    val req = cmd match {
      case LIST =>
        param = Option(param).getOrElse("")
        GetMonitorAppRequest(param)

      case CANCEL =>
        param = Option(param).getOrElse(throw new IllegalArgumentException("Need param AppId"))
        CancelMonitorAppRequest(param)

      case KILL =>
        param = Option(param).getOrElse(throw new IllegalArgumentException("Need param AppId"))
        KillMonitorAppRequest(param)

      case REGISTER =>
        logWarning("Please use spark-submit --monitor command.")
        param = Option(param).getOrElse(throw new IllegalArgumentException("Need param AppId SubmitParameter"))
        val ps = param.split(IFS)
        if(ps.length != 2) throw new IllegalArgumentException("Need param AppId SubmitParameter")
        MonitorAppRequest(ps(0),StartCommand(ps(1)))

      case SCHEDULED =>
        param = Option(param).getOrElse(throw new IllegalArgumentException("Need param jobName schedule parameter"))
        val ps = param.split(IFS)
        if(ps.length != 3) throw new IllegalArgumentException("Need param jobName schedule parameter")
        ScheduledAppRequest(ps(0),ps(1).toLong,StartCommand(ps(2)))

      case _ =>
        GetMonitorAppRequest("")
    }

    val yarnAppMonitorRef = YarnApplicationMonitorServer.createYarnAppMonitorCli(new SparkConf(), host, port)
    val res = yarnAppMonitorRef.askSync[MonitorAppResponse](req)
    logInfo(res.info)
  }

  @tailrec
  private def parse(args : List[String]): Unit = args match {
    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value,"Please use hostname " + value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case ("--command" | "-m") :: LIST :: value :: tail =>
      cmd = LIST
      param = value
      parse(tail)

    case ("--command" | "-m") :: CANCEL :: value :: tail =>
      cmd = CANCEL
      param = value
      parse(tail)

    case ("--command" | "-m") :: KILL :: value :: tail =>
      cmd = KILL
      param = value
      parse(tail)

    case ("--command" | "-m") :: REGISTER :: value1 :: value2 :: tail =>
      cmd = REGISTER
      param = value1+IFS+value2
      parse(tail)

    case ("--command" | "-m") :: SCHEDULED :: value1 :: value2 :: value3 :: tail =>
      cmd = SCHEDULED
      param = value1+IFS+value2+IFS+value3
      parse(tail)

    case ("-H" | "--help") :: tail =>printUsageAndExit(0)
    case Nil => //No-op
    case _ => printUsageAndExit(1)
  }

  private def printUsageAndExit(exitCode : Int): Unit = {
    System.err.println(
      "Usage: YarnApplicationMonitorServer [options]\n" +
        "\n" +
        "Options:\n" +
        "  -h HOST , --host HOST     Hostname to listen on\n" +
        "  -p PORT , --port PORT     Port to listen on (default: 7077)\n" +
        "  -m CMD  , --command CMD   Run command string (default: get)\n" +
        "                            CMD in [list,cancel,register,kill]\n" +
        s"                            $LIST [AppID or jobName]\n" +
        s"                            $CANCEL AppID or jobName\n" +
        s"                            $REGISTER AppID SubmitAppParam\n" +
        s"                            $KILL AppID\n" +
        s"                            $SCHEDULED jobName scheduleInterval SubmitAppParam\n" +
        "  -H, --help                Print help info.\n" +
        "Example:\n" +
        "  spark-monitor -m cancel appID\n" +
        "  spark-monitor -h 127.0.0.1 -p 2233 -m list \"\"\n" +
        "  spark-monitor -m schedule sync2mysql 3600 parameters\n" +
        "  spark-monitor -h 127.0.0.1 -p 2233 -m register appId submitParam")
    System.exit(exitCode)
  }
}

/**
  * status =< 3 表示被重启次数
  * status >= 10 表示schedule任务类型
  */
private[spark] case class AppStatus(status : Int,
                                    command: StartCommand)
/**
  * 声明StartCommand是保留扩展能力的
  */
private[spark] case class StartCommand(command : String)

/**
  *  MonitorApp 的所有request类型
  */
private[spark] trait MonitorAppReq
private[spark] case class MonitorAppRequest(appId: String,
                                            command: StartCommand) extends MonitorAppReq
private[spark] case class KillMonitorAppRequest(appId: String) extends MonitorAppReq
private[spark] case class CancelMonitorAppRequest(appId: String) extends MonitorAppReq
private[spark] case class GetMonitorAppRequest(appId: String) extends MonitorAppReq
private[spark] case class ScheduledAppRequest(jobName: String,
                                              scheduled: Long,
                                              command: StartCommand) extends MonitorAppReq

/**
  * MonitorApp 的所有response类型
  */
private[spark] case class MonitorAppResponse(info : String,state : MonitorResponseResult)

/**
  * MonitorApp 的response所有返回结果类型
  */
private[spark] object MonitorResponseResult extends Enumeration {
  type MonitorResponseResult = Value
  val SUCCESS,FAILURE,UNKNOWN=Value
}
