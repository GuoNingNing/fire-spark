package org.fire.spark.streaming.core.kit

import java.io._
import java.util.Properties
import java.net.Proxy

import org.apache.spark.SparkException

import scala.collection.JavaConverters._
import scala.util.Try
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

  def httpReq(url: String,
              proxy: Proxy = Proxy.NO_PROXY): HttpRequest = Http(url).timeout(10000,10000).proxy(proxy)

  def httpPost(url : String,
               data : String,
               headers : Map[String,String] = Map.empty[String, String]): (Int,String) ={
    val res = httpReq(url).postData(data).headers(headers).asString
    (res.code,res.body)
  }

  def httpPost(url: String,
               data: Seq[(String, String)],
               headers: Map[String, String]): (Int,String) = {
    val res = httpReq(url).postForm(data).headers(headers).asString
    (res.code, res.body)
  }

  def httpGet(url : String,param : Seq[(String,String)]) : (Int,String) = {
    val req = Http(url).params(param)
    val res = req.asString
    (res.code,res.body)
  }

  def httpGetFile(url: String, filename: String): Boolean = {
    val res = httpReq(url).execute { is =>
      saveToFile[InputStream](filename, is, (in,out) => {
        val buffer = new Array[Byte](4096)
        Iterator.continually(in.read(buffer)).takeWhile(-1 !=).foreach(c => out.write(buffer,0,c))
      })
    }
    res.body
  }

  def saveToFile[T](fileName: String, content: T, writeFile: (T, OutputStream) => Unit): Boolean = {
    Try {
      val fos = new java.io.FileOutputStream(fileName)
      writeFile(content, fos)
      fos.flush()
      fos.close()
      true
    }.getOrElse(false)
  }

}