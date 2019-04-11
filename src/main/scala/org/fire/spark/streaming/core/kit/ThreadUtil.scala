package org.fire.spark.streaming.core.kit

import java.util.concurrent.LinkedBlockingDeque

import scala.concurrent.{Future, Promise}
import scala.util.control.NonFatal

/**
  * Created by cloud on 2019/01/18.
  */
object ThreadUtil {

  def asyncExec[T](f: => T): Future[T] = {
    val p = Promise[T]()
    new Thread() {
      override def run(): Unit = {
        try {
          p.success(f)
        } catch {
          case NonFatal(e) => p.failure(e)
        }
      }
    }.start()
    p.future
  }

  def multiThreadByKeyExec[T, R](iter: Iterator[(String, T)],
                                 fun: Iterator[T] => Iterator[R],
                                 number: Int = 2,
                                 capacity: Int = 10): Iterator[R] = {
    multiThreadGroupingExec[(String, T), R](
      iter,
      iterator => fun(iterator.map(_._2)),
      t => t._1.hashCode() & 0x7FFFFFFF,
      number,
      capacity
    )
  }

  def multiThreadExec[T, R](iter: Iterator[T],
                            fun: Iterator[T] => Iterator[R],
                            number: Int = 2,
                            capacity: Int = 10): Iterator[R] =
    multiThreadGroupingExec[T, R](iter, fun, t => t.hashCode() & 0x7FFFFFFF, number, capacity)

  def multiThreadGroupingExec[T, R](iter: Iterator[T],
                                    fun: Iterator[T] => Iterator[R],
                                    grouping: T => Long,
                                    number: Int = 2,
                                    capacity: Int = 10): Iterator[R] = {
    val pushQueues = 1 to number map (_ => new LinkedBlockingDeque[Option[T]](capacity)) toList
    val pullQueue = new LinkedBlockingDeque[Option[R]](capacity * 2)

    val threads = 1 to number map { x =>
      new Thread() {
        override def run(): Unit = {
          fun(new QueueIterator[T](pushQueues(x - 1))).foreach(x => pullQueue.put(Some(x)))
        }
      }
    }

    threads.foreach(_.start())
    new Thread() {
      override def run(): Unit = {
        iter.foreach(x => pushQueues((grouping(x) % number).toInt).put(Some(x)))
        pushQueues.foreach(_.put(None))
        threads.foreach(_.join())
        pullQueue.put(None)
      }
    }.start()

    new QueueIterator[R](pullQueue)
  }

  def multiOperateThreadExec[T, S, R](iter: Iterator[T],
                                      firstOperate: Iterator[T] => Iterator[S],
                                      operates: List[Iterator[S] => Iterator[S]],
                                      lastOperate: Iterator[S] => Iterator[R],
                                      capacity: Int = 100): Iterator[R] = {
    val inputQueues = operates.map(_ => new LinkedBlockingDeque[Option[S]](capacity)) :+ new LinkedBlockingDeque[Option[S]](capacity)
    val firstOutputQueue = inputQueues.head
    val lastInputQueue = inputQueues.last

    val threads = operates.zipWithIndex.map { case (func, idx) => {
      new Thread() {
        override def run(): Unit = {
          func(new QueueIterator[S](inputQueues(idx))).foreach(x => inputQueues(idx + 1).put(Some(x)))
          inputQueues(idx+1).put(None)
        }
      }
    }}
    threads.foreach(_.start())

    val firstThread = new Thread() {
      override def run(): Unit = {
        firstOperate(iter).foreach(x => firstOutputQueue.put(Some(x)))
        firstOutputQueue.put(None)
      }
    }

    firstThread.start()

    lastOperate(new QueueIterator[S](lastInputQueue))
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
