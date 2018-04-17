package org.fire.spark.streaming.tool

import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.elasticsearch.spark.rdd.EsSpark
import org.fire.spark.streaming.core.{FireSpark, SQLContextSingleton}
import org.fire.spark.streaming.tool.HiveToEsApp.{esNodes, esPort, index}

import scala.language.postfixOps


/**
  * Created by guoning on 2017/5/19.
  *
  * Hive 数据 同步到 ES
  *
  */
object HiveToEsApp extends FireSpark {


  private var sql: String = _
  private var index: String = _
  private var _type: String = _
  private var dateFileds: String = _
  private var overwrite: Boolean = false


  private var number_of_shards = 7
  private var number_of_replicas = 1

  private val esParam = scala.collection.mutable.Map[String, String](
    "es.nodes" -> "es01",
    "es.port" -> "9200",
    "es.batch.size.entries" -> "10000",
    "es.index.auto.create" -> "true"
  )

  private def esNodes = esParam("es.nodes")

  private def esPort = esParam("es.port")


  override def handle(sc: SparkContext) = {

    val start = System.currentTimeMillis()

    var argv = args.toList

    logger.info(s"input options ${argv.length} \n ${argv.mkString("\n")}")

    while (argv.nonEmpty) {
      argv match {
        case ("--sql") :: value :: tail =>
          sql = value
          argv = tail
        case ("--index") :: value :: tail =>
          index = value
          argv = tail
        case ("--type") :: value :: tail =>
          _type = value
          argv = tail
        case ("--dateFileds") :: value :: tail =>
          dateFileds = value
          argv = tail
        case ("--number_of_shards") :: value :: tail =>
          number_of_shards = value.toInt
          argv = tail
        case ("--number_of_replicas") :: value :: tail =>
          number_of_replicas = value.toInt
          argv = tail
        case es :: value :: tail if es.startsWith("--es.") =>
          esParam += es.substring(2) -> value
          argv = tail
        case Nil =>
        case tail =>
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          printUsageAndExit()
      }
    }


    logger.info(s"ready to load data [$sql] to es [$index/${_type}]")

    val hiveContext = SQLContextSingleton.getHiveContext(sc)


    val logs = hiveContext.sql(sql)

    logs.show()


    val schema = logs.schema
    logger.info(s"get schema $schema")

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
               |   "type":"keyword"
               |}""".stripMargin
          }
      }
    })


    val cmdStr =
      s"""{
         |  "settings": {
         |    "index.mapping.ignore_malformed": "true",
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


    logger.info(s"create index ...\n$cmdStr")

    import scala.sys.process._

    val cmd = Seq("curl", "-H", "Content-Type: application/json", "-X", "PUT", "-d " + cmdStr, s"${esNodes.split(",").head}:$esPort/$index")

    val result = cmd !!

    logger.info(s"cmd $cmd\nresult $result")

    EsSpark.saveJsonToEs(logs.toJSON, s"$index/${_type}", esParam)

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
        |   --overwrite <default:false true 删除索引重建>
        |   --dateFileds <指定时间字段,多个可以 "逗号" 分割>
        |   --number_of_shards <分片数,默认为5>
        |   --number_of_replicas <副本数,默认为1>
        |   --es.* <es. 开头的为 传入 es 的配置>
        |""".stripMargin)
    System.exit(1)
  }

}

object Test {
  def main(args: Array[String]): Unit = {

    val mapping =
      """
        |{
        |  "settings": {
        |    "index.mapping.ignore_malformed": "true",
        |    "number_of_shards": 7,
        |    "number_of_replicas": 1
        |  },
        |  "mappings": {
        |    "ymd": {
        |      "properties": {
        |"total_commission_fee":{
        |   "type":"keyword"
        |},
        |"site_name":{
        |   "type":"keyword"
        |},
        |"site_id":{
        |   "type":"keyword"
        |},
        |"create_time":{
        |   "format": "yyyy-MM-dd HH:mm||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyyMMdd",
        |   "type": "date"
        |},
        |"subsidy_fee":{
        |   "type":"keyword"
        |},
        |"pay_price":{
        |   "type":"keyword"
        |},
        |"tk3rd_type":{
        |   "type":"keyword"
        |},
        |"earning_time":{
        |   "format": "yyyy-MM-dd HH:mm||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyyMMdd",
        |   "type": "date"
        |},
        |"order_type":{
        |   "type":"keyword"
        |},
        |"commission_rate":{
        |   "type":"keyword"
        |},
        |"commission":{
        |   "type":"keyword"
        |},
        |"price":{
        |   "type":"keyword"
        |},
        |"tk3rd_pub_id":{
        |   "type":"keyword"
        |},
        |"adzone_name":{
        |   "type":"keyword"
        |},
        |"alipay_total_price":{
        |   "type":"keyword"
        |},
        |"trade_id":{
        |   "type":"keyword"
        |},
        |"subsidy_type":{
        |   "type":"keyword"
        |},
        |"item_num":{
        |   "type":"keyword"
        |},
        |"adzone_id":{
        |   "type":"keyword"
        |},
        |"auction_category":{
        |   "type":"keyword"
        |},
        |"terminal_type":{
        |   "type":"keyword"
        |},
        |"item_title":{
        |   "type":"keyword"
        |},
        |"tk_status":{
        |   "type":"keyword"
        |},
        |"seller_nick":{
        |   "type":"keyword"
        |},
        |"num_iid":{
        |   "type":"keyword"
        |},
        |"subsidy_rate":{
        |   "type":"keyword"
        |},
        |"trade_parent_id":{
        |   "type":"keyword"
        |},
        |"income_rate":{
        |   "type":"keyword"
        |},
        |"total_commission_rate":{
        |   "type":"keyword"
        |},
        |"pub_share_pre_fee":{
        |   "type":"keyword"
        |},
        |"seller_shop_title":{
        |   "type":"keyword"
        |},
        |"title":{
        |   "type":"keyword"
        |},
        |"subtitle":{
        |   "type":"keyword"
        |},
        |"item_id":{
        |   "type":"keyword"
        |},
        |"raw_price":{
        |   "type":"double"
        |},
        |"zk_price":{
        |   "type":"double"
        |},
        |"description":{
        |   "type":"keyword"
        |},
        |"pic":{
        |   "type":"keyword"
        |},
        |"post_free":{
        |   "type":"long"
        |},
        |"month_sales":{
        |   "type":"long"
        |},
        |"comm_count":{
        |   "type":"long"
        |},
        |"order_count":{
        |   "type":"long"
        |},
        |"taobao_cate_ids":{
        |   "type":"keyword"
        |},
        |"platform_id":{
        |   "type":"long"
        |},
        |"source_id":{
        |   "type":"long"
        |},
        |"ticket_id":{
        |   "type":"long"
        |},
        |"cate_id":{
        |   "type":"long"
        |},
        |"subcate_id":{
        |   "type":"long"
        |},
        |"shop_id":{
        |   "type":"long"
        |},
        |"brand_id":{
        |   "type":"long"
        |},
        |"status":{
        |   "type":"long"
        |},
        |"improve":{
        |   "type":"long"
        |},
        |"is_recommend":{
        |   "type":"long"
        |},
        |"is_prior":{
        |   "type":"long"
        |},
        |"update_time":{
        |   "format": "yyyy-MM-dd HH:mm||yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyyMMdd",
        |   "type": "date"
        |},
        |"product_type":{
        |   "type":"long"
        |},
        |"cate_id3":{
        |   "type":"long"
        |},
        |"cate_id4":{
        |   "type":"long"
        |},
        |"ymd":{
        |   "type":"keyword"
        |}}
        |    }
        |  }
        |}
      """.stripMargin

    import scala.sys.process._
    val cmd = Seq("curl", "-H", "Content-Type: application/json", "-X", "PUT", "-d " + mapping, s"http://es01:9200/gn_test_v4")

    val result = cmd !!

    println(result)
  }
}
