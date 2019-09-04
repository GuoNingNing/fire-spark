import kafka.utils.ZKGroupTopicDirs
import org.I0Itec.zkclient.ZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable.ListBuffer

object KafkaTestDemoApp {

    import org.I0Itec.zkclient.serialize.ZkSerializer

    class MyZkSerializer extends ZkSerializer {
        override def serialize(o: Any): Array[Byte] = {

            String.valueOf(o).getBytes()
        }

        override def deserialize(bytes: Array[Byte]): String = {
            new String(bytes)
        }
    }

    def main(args: Array[String]): Unit = {

        val kafkaParams = Map[String, Object](
            "bootstrap.servers" -> "c1-dsj-kafka102.bj:9092,c1-dsj-kafka103.bj:9092,c1-dsj-kafka104.bj:9092,c1-dsj-kafka105.bj:9092,c1-dsj-kafka106.bj:9092,c1-dsj-kafka107.bj:9092,c1-dsj-kafka108.bj:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "zk2kfk-test",
            "auto.offset.reset" -> "latest", //也可以设置earliest和none，
            //将提交设置成手动提交false，默认true，自动提交到kafka，
            "enable.auto.commit" -> "false"
        )
        val group = "zk2kfk-test"
        val topics = "app_yimi_binlog,app_yingtao_binlog,app_inke_binlog"
        val zkQuorum = "c1-dsj-kafka102.bj:2181,c1-dsj-kafka103.bj:2181,c1-dsj-kafka104.bj:2181,c1-dsj-kafka105.bj:2181,c1-dsj-kafka106.bj:2181,c1-dsj-kafka107.bj:2181,c1-dsj-kafka108.bj:2181"

        val conf = new SparkConf().setAppName("zk2kfk").setMaster("local[4]")
        val streamingContext = new StreamingContext(conf, Seconds(5))

        val zkClient = new ZkClient(zkQuorum)
        zkClient.setZkSerializer(new MyZkSerializer)

        val strings = topics.split(",")

        var fromOffsets: Map[TopicPartition, Long] = Map()

        for (topic <- strings) {
            val topicDirs = new ZKGroupTopicDirs(group, topic)
            val zkTopicPath = s"${topicDirs.consumerOffsetDir}"
            println(zkTopicPath)
            val children = zkClient.countChildren(s"${zkTopicPath}")

            println(children)
            //
            for (i <- 0 until children) {
                val partitionOffset: String = zkClient.readData(s"${zkTopicPath}/${i}")
                val tp = new TopicPartition(topic, i)
                fromOffsets += (tp -> partitionOffset.toLong)
            }
        }


        fromOffsets.foreach(println)


        val Dstream = KafkaUtils.createDirectStream[String, String](streamingContext,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Assign[String, String](fromOffsets.keys, kafkaParams, fromOffsets)
        )
        val offsetRanges = new ListBuffer[OffsetRange]

        fromOffsets.foreach(x => {
            offsetRanges += OffsetRange(x._1, x._2, x._2)
        })

        Dstream.foreachRDD(rdd => {

            Dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges.toArray)
        })

        streamingContext.start()
        streamingContext.awaitTermination()

    }
}