import com.alibaba.fastjson.JSON;
import entity.Student;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import sink.MysqlSink;
import sink.PrintSinkFunction;

import java.util.Properties;

/**
 * 从kafka 拿数据 写入 mysql
 *
 * @data: 2019/7/20 1:02 PM
 * @author:lixiyan
 */
public class MainMysqlSink {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.put("bootstrap.servers","node001:9092");
        props.put("zookeeper.connect","node001:2181");
        props.put("group.id","test");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset","latest");


        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer<>(
                "tc",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(string -> JSON.parseObject(string, Student.class));

        student.addSink(new MysqlSink());
        student.addSink(new PrintSinkFunction<>());

        env.execute("MainMysqlSink");
    }
}
