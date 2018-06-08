package org.fire.spark.streaming.core.plugins.baidubce

import com.baidubce.auth.DefaultBceCredentials
import com.baidubce.services.bos.model.{GetObjectRequest, ListObjectsRequest}
import com.baidubce.services.bos.{BosClient, BosClientConfiguration}
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._

/**
  * Created by cloud on 18/6/6.
  */
case class BosObjectChunk(path: String, startPos: Long, endPos: Long)
case class BosConfig(key: String,
                     secretKey: String,
                     bucket: String,
                     endpoint: String)

class BosPartition(val rddId: Int,
                   override val index: Int,
                   val chunks: Seq[BosObjectChunk]) extends Partition

class BosRDD(sc: SparkContext,
             bosConfig: BosConfig,
             path: String,
             isFile: Boolean = true,
             numPartition: Int = 3) extends RDD[String](sc, Nil){

  private def createBosClient(): BosClient = {
    val config = new BosClientConfiguration()
    config.setCredentials(new DefaultBceCredentials(bosConfig.key, bosConfig.secretKey))
    config.setEndpoint(bosConfig.endpoint)

    new BosClient(config)
  }

  private def getListObjectChunk(bosClient: BosClient, path: String): Array[List[BosObjectChunk]] = {
    val listObjectReq = new ListObjectsRequest(bosConfig.bucket)
    listObjectReq.setPrefix(path)

    val listObject = bosClient.listObjects(listObjectReq)
    val totalSize = listObject.getContents.map(_.getSize).sum
    val chunkSize = totalSize/numPartition
    val tmpChunks = new Array[List[BosObjectChunk]](numPartition+1)
    var index = 0
    var chunks: List[BosObjectChunk] = List.empty[BosObjectChunk]
    var needSize: Long = 0L

    val computeSize = (i: Int) => {
      chunks = Option(tmpChunks(i)).getOrElse(List.empty[BosObjectChunk])
      needSize = chunkSize - chunks.map(x => x.endPos - x.startPos).sum
    }

    listObject.getContents.filter(_.getSize != 0).foreach { bos =>
      computeSize(index)
      if(needSize <= 0) {
        index += 1
        computeSize(index)
      }
      if(bos.getSize <= needSize){
        tmpChunks(index) = chunks :+ BosObjectChunk(bos.getKey, 0, bos.getSize)
      }else{
        var remainSize = bos.getSize
        var readySize = 0L
        while(remainSize >= chunkSize){
          tmpChunks(index) = chunks :+ BosObjectChunk(bos.getKey, readySize, readySize+needSize)
          remainSize -= needSize
          readySize += needSize
          index += 1
          computeSize(index)
        }
        if(remainSize > 0 && remainSize < needSize) {
          tmpChunks(index) = chunks :+ BosObjectChunk(bos.getKey, readySize, readySize+remainSize)
        }
      }
    }

    val chunkArray = new Array[List[BosObjectChunk]](numPartition)
    chunks = Option(tmpChunks.last).getOrElse(List.empty[BosObjectChunk])
    val chunkMap = chunks.map(x => x.path -> x).toMap
    for (i <- 0 until numPartition) {
      chunkArray(i) = tmpChunks(i)
    }
    val mergeEndChunks = chunkArray.last.map { chunk =>
      val endChunk = chunkMap.getOrElse(chunk.path, BosObjectChunk(chunk.path, 0, 0))
      BosObjectChunk(chunk.path, chunk.startPos, chunk.endPos+(endChunk.endPos-endChunk.startPos))
    }
    chunkArray(numPartition-1) = mergeEndChunks
    chunkArray
  }

  private def getObjectChunk(bosClient: BosClient, path: String): Array[List[BosObjectChunk]] = {
    val mateData = bosClient.getObjectMetadata(bosConfig.bucket,path)
    val length = mateData.getContentLength
    val chunkSize = length/numPartition
    val tailChunkSize = length%numPartition
    var startPos = 0L
    var endPos = chunkSize
    val chunks = new Array[List[BosObjectChunk]](numPartition)
    for (i <- 0 until numPartition-1) {
      chunks(i) = List(BosObjectChunk(path,startPos,endPos))
      startPos += chunkSize
      endPos += chunkSize
    }
    val bosObjectChunk = BosObjectChunk(path,startPos,endPos+tailChunkSize)
    chunks(numPartition-1) = List(bosObjectChunk)
    chunks
  }

  private def getChunks(bosClient: BosClient): Array[List[BosObjectChunk]] =
    if (isFile) getObjectChunk(bosClient, path) else getListObjectChunk(bosClient, path)


  override def getPartitions: Array[Partition] = {
    val client = createBosClient()
    val partitions = new Array[Partition](numPartition)
    val chunks = getChunks(client)
    chunks.zipWithIndex.foreach {
      case (list, index) => partitions(index) = new BosPartition(id, index, list)
    }
    partitions
  }

  override def compute(split: Partition, context: TaskContext): InterruptibleIterator[String] = {
    val bosPartition = split.asInstanceOf[BosPartition]
    val client = createBosClient()
    bosPartition.chunks.foreach(d =>
      log.info(s"compute ${d.startPos} -> ${d.endPos}")
    )
    val iter = bosPartition.chunks.iterator.flatMap { bosObjectChunk =>
      val start = bosObjectChunk.startPos
      val end = bosObjectChunk.endPos
      val bosBufferReadLine = new BosBufferReadLine(client, bosConfig.bucket, bosObjectChunk.path, start, end)
      Iterator.continually(bosBufferReadLine.readLine()).takeWhile(null !=)
    }
    new InterruptibleIterator[String](context, iter)
  }

}

class BosBufferReadLine(bosClient: BosClient,
                        bucket: String,
                        objectKey: String,
                        startPos: Long,
                        endPos: Long) {

  private val req = new GetObjectRequest(bucket,objectKey)
  req.setRange(startPos,endPos)

  private var bosObject = bosClient.getObject(req)
  private var in = bosObject.getObjectContent
  private var eol = false
  private var length = firstRead()

  private def firstRead(): Long = {
    if (startPos == 0) {
      startPos
    } else {
      var len = 0
      Iterator.continually(in.read()).takeWhile(s => s != -1 && s.toChar != '\n').foreach(s => len += 1)
      len+1
    }
  }

  private def nextInputStream(): Unit = {
    req.setRange(endPos+1, endPos+4096)
    bosObject = bosClient.getObject(req)
    in = bosObject.getObjectContent
    eol = true
  }

  def readLine(): String = {
    if (eol) {
      return null
    }

    var quit = false
    val line = new StringBuilder()

    while(!quit){
      val byte = in.read()
      if (byte == -1) {
        nextInputStream()
      } else if(byte.toChar == '\n') {
        length += 1
        if (length == endPos-startPos) {
          eol = true
        }
        quit = true
      } else {
        length += 1
        line.append(byte.toChar)
      }
    }
    line.toString()
  }

}

object BosImplicit {
  val BOS_PREFIX = "spark.baidubce.bos"
  val KEY = s"$BOS_PREFIX.key"
  val SECRET = s"$BOS_PREFIX.secret.key"
  val BUCKET = s"$BOS_PREFIX.bucket"
  val ENDPOINT = s"$BOS_PREFIX.endpoint"

  implicit class BosRDDImplicit(sparkContext: SparkContext) {

    def bosRDD(bosConfig: BosConfig,
               path: String,
               isFile: Boolean = true,
               numPartition: Int = 3): BosRDD = {
      new BosRDD(sparkContext, bosConfig, pathFormat(path), isFile, numPartition)
    }

    def bosFile(path: String, numPartition: Int = 3): BosRDD = {
      bosRDD(getBosConfig, path, numPartition = numPartition)
    }

    def bosDir(path: String, numPartition: Int = 3): BosRDD = {
      bosRDD(getBosConfig, path, false, numPartition)
    }

    private def getBosConfig = {
      val key = sparkContext.getConf.get(KEY)
      val secretKey = sparkContext.getConf.get(SECRET)
      val bucket = sparkContext.getConf.get(BUCKET)
      val endpoint = sparkContext.getConf.get(ENDPOINT)
      BosConfig(key,secretKey,bucket,endpoint)
    }

    private def pathFormat(path: String): String = {
      val p = if(path.startsWith("/")) path.substring(1) else path
      if(p.endsWith("/")) p.substring(0,p.length-1) else p
    }

  }
}
