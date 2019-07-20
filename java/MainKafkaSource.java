import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import utils.KafkaSource;

import java.util.Properties;

/**
 * TODO
 *
 * @data: 2019/7/19 9:35 PM
 * @author:lixiyan
 */
public class MainKafkaSource {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer source = KafkaSource.getJSONSource();
        DataStreamSource<String> dataStreamSource = env.addSource(source);
        dataStreamSource.print();
        env.execute("Flink add data source");
    }
}
