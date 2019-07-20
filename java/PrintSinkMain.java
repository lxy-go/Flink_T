import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import sink.PrintSinkFunction;

import java.util.Properties;

/**
 * TODO
 *
 * @data: 2019/7/20 11:55 AM
 * @author:lixiyan
 */
public class PrintSinkMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "node001:9092");
        props.put("zookeeper.connect", "node001:2181");
        props.put("group.id", "test");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");  //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest"); //value 反序列化

        DataStreamSource<String> dataSource = env.addSource(new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), props)).setParallelism(1);

//        dataSource.print();
        dataSource.addSink(new PrintSinkFunction<>());
        env.execute("PrintSinkMain");

    }
}
