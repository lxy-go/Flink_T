package demo.t1.streaming

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * Kafka连接器作为consume接受数据
  *
  * date 2019/7/19 10:32 AM
  *
  * @author lixiyan
  */
object KafkaConnectorConsumerApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 1. 定义kafka信息
    val topic = "maxwell"
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id","test")

    // 2.定义Kafka source
    val kafkaSource = new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),properties)

    // 3.添加source
    val data = env.addSource(kafkaSource)
    data.print()
    env.execute("KafkaConnectConsumeApp")

  }

}
