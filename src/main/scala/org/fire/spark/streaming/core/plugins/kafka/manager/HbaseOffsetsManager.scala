package org.fire.spark.streaming.core.plugins.kafka.manager

import org.apache.hadoop.hbase.{CellUtil, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Put, Scan, Table}
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp
import org.apache.hadoop.hbase.filter.{BinaryComparator, RowFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.fire.spark.streaming.core.plugins.hbase.HbaseConnPool

import scala.collection.JavaConversions._

/**
  * Created by guoning on 2017/10/20.
  *
  * Hbase 存储Offset
  */
class HbaseOffsetsManager(val sparkConf: SparkConf) extends OffsetsManager {


  private lazy val tableName = storeParams("hbase.table")

  private lazy val table: Table = {

    val conn = HbaseConnPool.connect(storeParams)

    if (!conn.getAdmin.tableExists(TableName.valueOf(tableName))) {
      val tableDesc: HTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName))

      tableDesc.addFamily(new HColumnDescriptor("topic_partition_offset"))

      conn.getAdmin.createTable(tableDesc)
    }
    conn.getTable(TableName.valueOf(tableName))

  }


  /** 存放offset的表模型如下，请自行优化和扩展，请把每个rowkey对应的record的version设置为1（默认值），因为要覆盖原来保存的offset，而不是产生多个版本
    * ----------------------------------------------------------------------------------------------------
    * rowkey            |  column family                                                          |
    * --------------------------------------------------------------------------
    * |                 |  column:topic(string)  |  column:partition(int)  | column:offset(long)  |
    * ----------------------------------------------------------------------------------------------
    * topic#partition   |   topic                |   partition             |    offset            |
    * ---------------------------------------------------------------------------------------------------
    */

  /**
    * 获取存储的Offset
    *
    * @param groupId
    * @param topics
    * @return
    */
  override def getOffsets(groupId: String, topics: Set[String]): Map[TopicPartition, Long] = {
    val offsets = topics.flatMap(
      topic => {

        val key = generateKey(groupId, topic)

        val filter = new RowFilter(CompareOp.GREATER_OR_EQUAL, new BinaryComparator(key.getBytes))

        val scan = new Scan().setFilter(filter)

        table.getScanner(scan).iterator().map(r => {
          val cells = r.rawCells()

          var topic = ""
          var partition = 0
          var offset = 0L

          cells.foreach(cell => {
            Bytes.toString(CellUtil.cloneQualifier(cell)) match {
              case "topic" => topic = Bytes.toString(CellUtil.cloneValue(cell))
              case "partition" => partition = Bytes.toInt(CellUtil.cloneValue(cell))
              case "offset" => offset = Bytes.toLong(CellUtil.cloneValue(cell))
              case other =>
            }
          })
          new TopicPartition(topic, partition) -> offset
        })

      })

    logInfo(s"getOffsets [$groupId,${offsets.mkString(",")}] ")

    offsets.toMap
  }

  /**
    * 更新 Offsets
    *
    * @param groupId
    * @param offsetInfos
    */
  override def updateOffsets(groupId: String, offsetInfos: Map[TopicPartition, Long]): Unit = {
    for ((tp, offset) <- offsetInfos) {

      import org.apache.hadoop.hbase.util.Bytes
      val put: Put = new Put(Bytes.toBytes(s"${generateKey(groupId, tp.topic)}#${tp.partition}"))
      put.addColumn(Bytes.toBytes("topic_partition_offset"), Bytes.toBytes("topic"), Bytes.toBytes(tp.topic))
      put.addColumn(Bytes.toBytes("topic_partition_offset"), Bytes.toBytes("partition"), Bytes.toBytes(tp.partition))
      put.addColumn(Bytes.toBytes("topic_partition_offset"), Bytes.toBytes("offset"), Bytes.toBytes(offset))

      table.put(put)

    }
    logInfo(s"updateOffsets [ $groupId,${offsetInfos.mkString(",")} ]")
  }
}
