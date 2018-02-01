package org.fire.spark.streaming.core.kit

import java.io.{File, FileInputStream, IOException, InputStreamReader}
import java.util.Properties

import org.apache.spark.SparkException

import scala.collection.JavaConverters._
import scala.collection.Map

import scalaj.http._

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

  def httpPost(url : String,data : String,headers : Map[String,String] = Map.empty[String,String]): (Int,String) ={
    val req = Http(url).postData(data)
    headers.foreach {case (k,v) => req.header(k,v)}
    val res = req.asString
    (res.code,res.body)
  }

}