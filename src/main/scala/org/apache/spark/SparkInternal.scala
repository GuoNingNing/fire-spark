package org.apache.spark

import org.apache.spark.rdd.{MapPartitionsRDD, RDD, RDDOperationScope}
import org.apache.spark.util.ClosureCleaner

import scala.reflect.ClassTag

/**
  * Created by cloud on 18/4/21.
  *
  * 这是用来将spark内部的底层的一些东西暴露出来的包
  * 可以提供一些更底层更自定义的实现
  *
  * @deprecated 不建议轻易使用,对spark内部不了解的话很容易出现bug和错误
  */
@deprecated
object SparkInternal {

  /**
    * 这是sparkContext 内部的对应的withScope方法
    */
  def withScope[T: ClassTag](sparkContext: SparkContext)(body: => T) = RDDOperationScope.withScope(sparkContext)(body)

  /**
    * 这是创建一个 spark 内部的MapPartitionsRDD对象的方法
    */
  def newMapPartitionsRDD[U: ClassTag, T: ClassTag](prev: RDD[T],
                                                    f: (TaskContext, Int, Iterator[T]) => Iterator[U],
                                                    preservesPartitioning: Boolean = false
                                                   ) = new MapPartitionsRDD[U,T](prev,f,preservesPartitioning)

  /**
    *
    * 这是sparkContext 内部的对应的clean方法
    */
  def clean[F <: AnyRef](f: F, checkSerializable: Boolean = true): F = {
    ClosureCleaner.clean(f, checkSerializable)
    f
  }

}
