# Fire-Spark
    让Spark开发变得更简单
### BUILD和使用方法
##### 1.构建方法

```
    $git clone https://github.com/GuoNingNing/fire-spark.git
    $cd fire-spark
    $mvn install clean package -DskipTests

    构建完成之后会将jar安装到m2的对应路径下，使用时在自己的项目的pom.xml文件里添加
    
    <dependency>
        <groupId>org.fire.spark.streaming</groupId>
    	<artifactId>fire-spark</artifactId>
    	<version>2.1.0_kafka-0.10</version>
    </dependency>
```

##### 2.配置说明

```properties

################################################
#
# 统一 配置
#
################################################
# 滑动窗口(单位:秒)
spark.batch.duration=5
# 启动入口
spark.main.class=org.demo.ReadKafkaDemoA
# 传入参数
spark.main.params=
# App 名字
spark.app.name=ReadKafkaDemoA-test
# 部署模式
spark.master=yarn
spark.deploy.mode=cluster
# driver 节点内存分配
spark.driver.memory=512M
# 单个 executor 分配核心数
spark.executor.cores=3
# 单个 executor 申请 JVM Heap 大小
spark.executor.memory=5G
# 启用 External shuffle Service服务
spark.shuffle.service.enabled=true
# Shuffle Service服务端口，必须和yarn-site中的一致
spark.shuffle.service.port=7337
# 开启动态资源分配
spark.dynamicAllocation.enabled=true
# 每个Application最小分配的executor数
spark.dynamicAllocation.minExecutors=1
# 每个Application最大并发分配的executor数
spark.dynamicAllocation.maxExecutors=30
spark.dynamicAllocation.schedulerBacklogTimeout=1s
spark.dynamicAllocation.sustainedSchedulerBacklogTimeout=5s


################################################
#
# Kafka Source 配置
#
# 格式：spark.source.kafka.consume.{标准Kafka配置}
# 程序会自动截取前缀 spark.source.kafka.consume. 后注入到kafkaParams
#
################################################
#broker 地址
spark.source.kafka.consume.bootstrap.servers=hadoop102:9092,hadoop103:9092,hadoop104:9092,hadoop105:9092,hadoop106:9092,hadoop107:9092,hadoop108:9092
# 消费组
spark.source.kafka.consume.group.id=gn_test
# 消费Topic 2
spark.source.kafka.consume.topics=topic_a
#首次消费 读取位置 latest, earliest, none
spark.source.kafka.consume.auto.offset.reset=latest
spark.source.kafka.consume.socket.timeout.ms=120000
spark.source.kafka.consume.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
spark.source.kafka.consume.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

################################################
#
# Offset 管理配置，默认支持redis，hbase来储存offset
# 不配置，则表示不进行offset管理，类似0.8的匿名消费
#
################################################
spark.source.kafka.offset.store.type=redis
spark.source.kafka.offset.store.redis.hosts=localhost
spark.source.kafka.offset.store.redis.port=6379

```
  
##### 3.示例代码  
  
```scala    
package z.cloud.t3

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.fire.spark.streaming.core.FireStreaming
import org.fire.spark.streaming.core.sources.KafkaDirectSource

object Demon extends FireStreaming {
    override def handle(ssc : StreamingContext): Unit = {
        val source = new KafkaDirectSource[String,String](ssc)
        //val conf = ssc.sparkContext.getConf
        source.getDStream.foreachRDD((rdd,time) => {
            rdd.take(10).foreach(println)
            source.updateOffsets(time.milliseconds)
        })
    }
}
```
