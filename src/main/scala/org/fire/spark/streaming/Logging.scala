package org.fire.spark.streaming

import org.slf4j.impl.StaticLoggerBinder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
/**
  * Created by cloud on 18/4/12.
  * 同spark.Logging 使用方式一致
  */
trait Logging {

  @transient private var log_ : Logger = null

  protected def logName = {
    this.getClass.getName.stripSuffix("$")
  }

  protected def log: Logger = {
    if (log_ == null) {
      initializeLogIfNecessary(false)
      log_ = LoggerFactory.getLogger(logName)
    }
    log_
  }

  protected def logInfo(msg: => String) {
    if (log.isInfoEnabled) log.info(msg)
  }

  protected def logDebug(msg: => String) {
    if (log.isDebugEnabled) log.debug(msg)
  }

  protected def logTrace(msg: => String) {
    if (log.isTraceEnabled) log.trace(msg)
  }

  protected def logWarning(msg: => String) {
    if (log.isWarnEnabled) log.warn(msg)
  }

  protected def logError(msg: => String) {
    if (log.isErrorEnabled) log.error(msg)
  }

  protected def logInfo(msg: => String, throwable: Throwable) {
    if (log.isInfoEnabled) log.info(msg, throwable)
  }

  protected def logDebug(msg: => String, throwable: Throwable) {
    if (log.isDebugEnabled) log.debug(msg, throwable)
  }

  protected def logTrace(msg: => String, throwable: Throwable) {
    if (log.isTraceEnabled) log.trace(msg, throwable)
  }

  protected def logWarning(msg: => String, throwable: Throwable) {
    if (log.isWarnEnabled) log.warn(msg, throwable)
  }

  protected def logError(msg: => String, throwable: Throwable) {
    if (log.isErrorEnabled) log.error(msg, throwable)
  }

  protected def initializeLogIfNecessary(isInterpreter: Boolean): Unit = {
    if (!Logging.initialized) {
      Logging.initLock.synchronized {
        if (!Logging.initialized) {
          initializeLogging(isInterpreter)
        }
      }
    }
  }

  private def initializeLogging(isInterpreter: Boolean): Unit = {
    StaticLoggerBinder.getSingleton.getLoggerFactoryClassStr
    Logging.initialized = true
    log
  }
}

private object Logging {
  @volatile private var initialized = false
  val initLock = new Object()
}
