package org.fire.spark.streaming.core.kit

import java.util.concurrent.LinkedBlockingDeque

import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

/**
  * Created by cloud on 2019/01/19.
  */
object ThreadUtil {

  def asyncExec[T](f: => T): Future[T] = {
    val p = Promise[T]()
    new Thread(){
      override def run(): Unit = try {
        p.success(f)
      } catch {
        case NonFatal(e) => p.failure(e)
      }
    }.start()
    p.future
  }

  def multiThreadExec[T, R](iter: Iterator[T],
                            fun: Iterator[T] => Iterator[R],
                            number: Int = 2,
                            capacity: Int = 10): Iterator[R] = {
    val pushQueues = 1 to number map(x => new LinkedBlockingDeque[Option[T]](capacity)) toList
    val pullQueue = new LinkedBlockingDeque[Option[R]](capacity*2)

    val threads = 1 to number map { x =>
      new Thread() {
        override def run(): Unit = {
          fun(new QueueIterator[T](pushQueues(x-1))).foreach(x => pullQueue.put(Some(x)))
        }
      }
    }

    threads.foreach(_.start())
    new Thread() {
      override def run(): Unit = {
        iter.foreach(x => pushQueues((x.hashCode() & 0x7FFFFFFF)%number).put(Some(x)))
        pushQueues.foreach(_.put(None))
        threads.foreach(_.join())
        pullQueue.put(None)
      }
    }.start()

    new QueueIterator[R](pullQueue)
  }

  private class QueueIterator[T](queue: LinkedBlockingDeque[Option[T]]) extends Iterator[T] {
    var t: Option[T] = _

    override def hasNext: Boolean = {
      t = queue.take()
      t.isDefined
    }

    override def next(): T = t.get
  }

}
