package demo.t1.streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper


/**
  *  Kafka作为Producer
  *
  *
  * @author lixiyan
*/
object KafkaConnnectorProducerAPP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 从socket接受数据，通过flink将数据sink到kafka
    val data = env.socketTextStream("localhost",12345)
    // 2. 配置Kafka属性
    val topic = "test"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","node001:9092")
    // 3.构造器 topic - 序列化器 - kafka属性 —— Flink语义
    val kafkaSink: FlinkKafkaProducer[String] = new FlinkKafkaProducer[String](topic,new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE)

    // 4. 数据输出到kafka，让kafka成为producer输出
    data.addSink(kafkaSink)

    env.execute("KafkaConnecorProducerApp")

  }

}
