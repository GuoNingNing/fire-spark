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
##### 2.示例代码    
```    
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
