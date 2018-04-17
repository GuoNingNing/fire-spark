package org.fire.spark.streaming.core.kit

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.FilterList.Operator
import org.apache.hadoop.hbase.filter.{Filter, FilterList, PageFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.fire.spark.streaming.core.plugins.hbase.HbaseConnPool
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

case class HbaseKit(configuration: Configuration) {
  private lazy val logger = LoggerFactory.getLogger(getClass)


  /*Habse 连接 */
  private lazy val hConnection: Connection = HbaseConnPool.connect(configuration)
  /*Habse Admin*/
  private lazy val hAdmin: Admin = hConnection.getAdmin

  private lazy val UNLIMIT: Long = -1l
  private lazy val NOSKIP: Long = -1l

  /**
    * 表是否存在
    *
    * @param tableName
    * @return
    */
  def isExist(tableName: String): Boolean = {
    hAdmin.tableExists(TableName.valueOf(tableName))
  }

  /**
    * 创建表
    *
    * @param tableName
    * @param columnFamilys
    */
  def createTable(tableName: String, columnFamilys: Array[String]): Unit = {
    if (hAdmin.tableExists(TableName.valueOf(tableName))) {
      logger.info(s"表 [ $tableName ] 已经存在")
    } else {
      val tableDesc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))
      for (columnFaily <- columnFamilys) {
        tableDesc.addFamily(new HColumnDescriptor(columnFaily))
      }
      hAdmin.createTable(tableDesc)
      logger.info(s"表 [ $tableName ] 创建成功")
    }
  }

  /**
    * 删除表
    *
    * @param tableName
    */
  def deleteTable(tableName: String): Unit = {
    if (hAdmin.tableExists(TableName.valueOf(tableName))) {
      hAdmin.disableTable(TableName.valueOf(tableName))
      hAdmin.deleteTable(TableName.valueOf(tableName))
      logger.info(s"删除表 [ $tableName ] 成功!")
    } else {
      logger.info(s"表 [ $tableName ] 不存在!")
    }
  }


  /**
    * 添加一行数据
    *
    * @param tableName   表名
    * @param row         row
    * @param columnFaily 列簇
    * @param column      列
    * @param value       值
    */
  def addRow(tableName: String, row: String, columnFaily: String, column: String, value: String): Unit = {
    val _table = hConnection.getTable(TableName.valueOf(tableName))
    val put: Put = new Put(Bytes.toBytes(row))
    put.addColumn(Bytes.toBytes(columnFaily), Bytes.toBytes(column), Bytes.toBytes(value))
    _table.put(put)
  }

  /**
    * 单行删除
    *
    * @param tableName
    * @param row
    * @return
    */
  def delRow(tableName: String, row: String): Boolean = {
    val hTable = hConnection.getTable(TableName.valueOf(tableName))
    val delete: Delete = new Delete(Bytes.toBytes(row))
    try {
      hTable.delete(delete)
      true
    } catch {
      case e: HBaseIOException =>
        logger.error(e.getMessage)
        false
    }
  }


  /**
    * 多行删除
    *
    * @param tableName
    * @param rows
    */
  def delMultiRows(tableName: String, rows: Array[String]): Unit = {
    val hTable = hConnection.getTable(TableName.valueOf(tableName))
    val deleteArray = for (row <- rows) yield new Delete(Bytes.toBytes(row))
    try {
      hTable.delete(deleteArray.toList)
      true
    } catch {
      case e: HBaseIOException =>
        logger.error(e.getMessage)
        false
    }
  }


  /**
    * 获取指定一行数据
    *
    * @param tableName
    * @param row
    */
  def getRow(tableName: String, row: String): Result = {
    val hTable = hConnection.getTable(TableName.valueOf(tableName))
    val get: Get = new Get(Bytes.toBytes(row))
    hTable.get(get)
  }

  /**
    * 获取所有数据
    *
    * @param tableName
    * @return
    */
  def getAllRows(tableName: String): Iterator[Result] = {
    val hTable = hConnection.getTable(TableName.valueOf(tableName))
    val scan = new Scan()
    val results: ResultScanner = hTable.getScanner(scan)
    results.iterator()
  }

  /**
    * rowkey多条查询
    *
    * @param tableName
    * @param rows
    * @return
    */
  def doGet(tableName: String, rows: List[String]): List[Result] = {
    val hTable = hConnection.getTable(TableName.valueOf(tableName))
    val getArray = for (row <- rows) yield new Get(Bytes.toBytes(row))
    hTable.get(getArray).toList
  }

  def doGet(tableName: String, rowKey: String): Result = {
    doGet(tableName, Bytes.toBytes(rowKey))
  }

  /**
    * rowkey单条查询
    *
    * @param tableName
    * @param rowKey
    * @return
    */
  def doGet(tableName: String, rowKey: Array[Byte]): Result = {
    val hTable = hConnection.getTable(TableName.valueOf(tableName))

    val gt: Get = new Get(rowKey)
    hTable.get(gt)
  }

  def scan(tableName: String, scn: Scan): Iterator[Result] = this.scan(tableName, scn, UNLIMIT, NOSKIP)

  def scan(tableName: String, startRow: String, stopRow: String): Iterator[Result] = scan(tableName, Bytes.toBytes(startRow), Bytes.toBytes(stopRow), UNLIMIT, NOSKIP)

  def scan(tableName: String, startRow: String, stopRow: String, limit: Long): Iterator[Result] = scan(tableName, Bytes.toBytes(startRow), Bytes.toBytes(stopRow), limit, NOSKIP)

  def scan(tableName: String, startRow: String, stopRow: String, limit: Long, skip: Long): Iterator[Result] = scan(tableName, Bytes.toBytes(startRow), Bytes.toBytes(stopRow), limit, skip)

  def scan(tableName: String, startRow: String, stopRow: String, limit: Long, skip: Long, filter: Filter): Iterator[Result] = scan(tableName, Bytes.toBytes(startRow), Bytes.toBytes(stopRow), limit, skip, filter)

  /**
    * @param startRow
    * @param stopRow
    * @param skip  跳过多少条
    * @param limit 分页大小
    * @return
    */
  def scan(tableName: String, startRow: Array[Byte], stopRow: Array[Byte], limit: Long, skip: Long): Iterator[Result] = {
    val scn = new Scan(startRow, stopRow)
    val filter = this.getPageFilter(limit, skip)
    if (filter != null) scn.setFilter(filter)
    scan(tableName, scn, skip)
  }

  /**
    * @param tableName
    * @param startRow
    * @param stopRow
    * @param skip  跳过多少条
    * @param limit 分页大小
    * @param filter
    * @return
    */
  def scan(tableName: String, startRow: Array[Byte], stopRow: Array[Byte], limit: Long, skip: Long, filter: Filter): Iterator[Result] = {
    val scn = new Scan(startRow, stopRow)

    val filterList: FilterList = new FilterList(Operator.MUST_PASS_ALL)
    val pageFilter: Filter = this.getPageFilter(limit, skip)

    if (pageFilter != null) filterList.addFilter(pageFilter)
    if (filter != null) filterList.addFilter(filter)
    if (!filterList.getFilters.isEmpty) scn.setFilter(filterList)

    scan(tableName, scn, skip)
  }

  /**
    * 带scan的范围查询
    *
    * @param tableName
    * @param scn
    * @param limit
    * @param skip
    * @return
    */
  def scan(tableName: String, scn: Scan, limit: Long, skip: Long): Iterator[Result] = {
    val pageFilter: Filter = this.getPageFilter(limit, skip)

    if (scn.getFilter != null) {
      val filterList: FilterList = new FilterList(Operator.MUST_PASS_ALL)
      val filter: Filter = scn.getFilter
      filterList.addFilter(pageFilter)
      filterList.addFilter(filter)
      scn.setFilter(filterList)
    } else {
      scn.setFilter(pageFilter)
    }

    scan(tableName, scn, skip)
  }

  /**
    * 真正读取记录操作
    *
    * @param tableName
    * @param scan
    * @param skip
    * @return
    */
  def scan(tableName: String, scan: Scan, skip: Long): Iterator[Result] = {
    val hTable = hConnection.getTable(TableName.valueOf(tableName))
    val rs: ResultScanner = hTable.getScanner(scan)
    if (rs != null && this.skip(rs, skip)) {
      rs.toIterator
    } else null
  }

  /**
    * 跳过多少条记录
    *
    * @param rs
    * @param rows
    * @return
    */
  def skip(rs: ResultScanner, rows: Long): Boolean = {
    if (rows <= 0) return true
    var skip = 0
    while (skip < rows && rs.next != null) skip += 1
    skip == rows
  }

  /**
    * 分页过滤器
    *
    * @param limit
    * @param skip
    * @return
    */
  def getPageFilter(limit: Long, skip: Long): Filter = {
    if (limit >= 0) {
      val pageSize = if (skip >= 0) limit + skip else limit
      val filter = new PageFilter(pageSize)
      return filter
    }
    null
  }

  /**
    * @param tableName
    * @param scn
    * @return
    */
  def getTotal(tableName: String, scn: Scan): Long = {
    var total: Long = 0l
    val hTable = hConnection.getTable(TableName.valueOf(tableName))
    val rs: ResultScanner = hTable.getScanner(scn)
    while (rs != null && rs.next() != null) total += 1l
    total
  }
}

object HbaseKit {
  def main(args: Array[String]): Unit = {

    val conf = HBaseConfiguration.create
    conf.set("hbase.master", "bigdata01:60000")
    conf.set("hbase.zookeeper.quorum", "bigdata01,bigdata02,bigdata03,bigdata04,bigdata05")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val row = HbaseKit(conf).getRow("taobao_order_v6", "a897fb146429146595597574")
    val map = row.getFamilyMap("cf".getBytes)

    for(key <- map.keySet()){
      println(new String(map.get(key)))
    }


  }
}
