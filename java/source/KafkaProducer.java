package source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.util.Properties;

/**
 * kafka的生产者
 *
 * @author lixiyan
 * @data 2019/8/27 1:45 PM
 */
public class KafkaProducer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.addSource(new MyNoParalleSource()).setParallelism(1);
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","172.16.206.133:9092");
        FlinkKafkaProducer010<String> producer010 = new FlinkKafkaProducer010<>("test", new SimpleStringSchema(), props);

        text.addSink(producer010);
        env.execute("KafkaProducer");
    }
}
