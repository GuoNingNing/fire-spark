package org.fire.spark.streaming.tool

import org.apache.spark.sql.types._
import org.apache.spark.{Logging, SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.EsSpark
import org.fire.spark.streaming.core.SQLContextSingleton

import scala.language.postfixOps


/**
  * Created by guoning on 2017/5/19.
  *
  * Hive 数据 同步到 ES
  *
  */
object HiveToEsApp extends Logging {


  private var sql: String = _
  private var index: String = _
  private var _type: String = _
  private var dateFileds: String = _


  private var number_of_shards = 5
  private var number_of_replicas = 1

  private val esParam = scala.collection.mutable.Map[String, String](
    "es.nodes" -> "localhost",
    "es.port" -> "9200",
    "es.batch.size.entries" -> "10000",
    "es.index.auto.create" -> "true"
  )

  private def esNodes = esParam("es.nodes")

  private def esPort = esParam("es.port")


  def main(args: Array[String]): Unit = {

    val start = System.currentTimeMillis()

    var argv = args.toList

    while (argv.nonEmpty) {
      argv match {
        case ("-sql") :: value :: tail =>
          sql = value
          argv = tail
        case ("-index") :: value :: tail =>
          index = value
          argv = tail
        case ("-type") :: value :: tail =>
          _type = value
          argv = tail
        case ("-dateFileds") :: value :: tail =>
          dateFileds = value
          argv = tail
        case ("-number_of_shards") :: value :: tail =>
          number_of_shards = value.toInt
          argv = tail
        case ("-number_of_replicas") :: value :: tail =>
          number_of_replicas = value.toInt
          argv = tail
        case es :: value :: tail if es.startsWith("-es.") =>
          esParam += es.substring(1) -> value
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          printUsageAndExit()
      }
    }


    logInfo(s"ready to load data [$sql] to es [$index/${_type}]")

    val conf = new SparkConf().setAppName("test")

    val sc = new SparkContext(conf)

    val hiveContext = SQLContextSingleton.getHiveContext(sc)


    val logs = hiveContext.sql(sql)

    val count = logs.count()

    logs.show()


    val schema = logs.schema
    logInfo(s"get schema $schema")

    val properties = schema.map(field => {

      field.dataType match {

        case IntegerType =>
          s"""
             |"${field.name}":{
             |   "type":"long"
             |}""".stripMargin
        case LongType =>
          s"""
             |"${field.name}":{
             |   "type":"long"
             |}""".stripMargin
        case DoubleType =>
          s"""
             |"${field.name}":{
             |   "type":"double"
             |}""".stripMargin
        case FloatType =>
          s"""
             |"${field.name}":{
             |   "type":"double"
             |}""".stripMargin
        case DateType =>
          s"""
             |"${field.name}":{
             |   "format": "yyyy-MM-dd HH:mm||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd",
             |   "type": "date"
             |}""".stripMargin
        case _ =>
          // 指定日期格式化
          if (dateFileds.contains(field.name)) {
            s"""
               |"${field.name}":{
               |   "format": "yyyy-MM-dd HH:mm||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyyMMdd",
               |   "type": "date"
               |}""".stripMargin
          } else {
            s"""
               |"${field.name}":{
               |   "index":"not_analyzed",
               |   "type":"string"
               |}""".stripMargin
          }
      }
    })


    val cmdStr =
      s"""{
         |  "settings": {
         |    "number_of_shards": $number_of_shards,
         |    "number_of_replicas": $number_of_replicas
         |  },
         |  "mappings": {
         |    "${_type}": {
         |      "properties": ${properties.mkString("{", ",", "}")}
         |    }
         |  }
         |}
         |""".stripMargin


    logInfo(s"create index ...\n$cmdStr")

    import scala.sys.process._

    val cmd = Seq("curl", "-L", "-X", "PUT", "-H", "'Content-Type: application/json'", "-d " + cmdStr, s"${esNodes.split(",").head}:$esPort/$index")

    val result = cmd !!

    logInfo(s"cmd $cmd\nresult $result")

    EsSpark.saveJsonToEs(logs.toJSON, s"$index/${_type}", esParam)

    logInfo(s"Write $count to es use time ${System.currentTimeMillis() - start}")

    sc.stop()
  }

  private def printUsageAndExit() = {

    System.err.println(
      """
        |"Usage: FireStreaming [options]
        |
        | Options are:
        |   --sql <执行 sql>
        |   --index <es 索引>
        |   --type < es type>
        |   --dateFileds <指定时间字段,多个可以 "逗号" 分割>
        |   --number_of_shards <分片数,默认为5>
        |   --number_of_replicas <副本数,默认为1>
        |   --es.* <es. 开头的为 传入 es 的配置>
        |""".stripMargin)
    System.exit(1)
  }

}
