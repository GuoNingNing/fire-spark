package org.fire.spark.streaming.core.kit

import java.io.{File, FileInputStream, IOException, InputStreamReader}
import java.lang.reflect.Constructor
import java.util.{Properties, ResourceBundle}

import org.apache.spark.{SparkConf, SparkContext, SparkException}
import org.apache.spark.scheduler.SparkListenerInterface
import org.apache.spark.util.Utils
import org.fire.spark.streaming.core.plugins.kafka.manager.{KafkaManager, OffsetsManager}

import scala.collection.Map
import scala.collection.JavaConverters._

/**
  * Created by guoning on 2017/5/19.
  */
object Utils {


  /** Load properties present in the given file. */
  def getPropertiesFromFile(filename: String): Map[String, String] = {
    val file = new File(filename)
    require(file.exists(), s"Properties file $file does not exist")
    require(file.isFile, s"Properties file $file is not a normal file")

    val inReader = new InputStreamReader(new FileInputStream(file), "UTF-8")
    try {
      val properties = new Properties()
      properties.load(inReader)
      properties.stringPropertyNames().asScala.map(
        k => (k, properties.getProperty(k).trim)).toMap
    } catch {
      case e: IOException =>
        throw new SparkException(s"Failed when loading Spark properties from $filename", e)
    } finally {
      inReader.close()
    }
  }

  def classForName(className: String): Class[_] = {
    Class.forName(className, true, Thread.currentThread().getContextClassLoader)
  }

  def main(args: Array[String]): Unit = {

  }

}