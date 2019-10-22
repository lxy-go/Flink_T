package utils;

import com.alibaba.fastjson.JSON;
import entity.Student;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * TODO
 * <p>
 * data  2019/7/20 3:40 PM
 * author lixiyan
 */
public class KafkaSource {

    public static  FlinkKafkaConsumer getJSONSource(){
        Properties props = new Properties();
        props.put("bootstrap.servers","node001:9092");
        props.put("zookeeper.connect","node001:2181");
        props.put("group.id","test1");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset","latest");

        FlinkKafkaConsumer  kafkaSource = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), props);
        return kafkaSource;
    }
    public static SingleOutputStreamOperator getObjectConsume(StreamExecutionEnvironment env){
        Properties props = new Properties();
        props.put("bootstrap.servers","node001:9092");
        props.put("zookeeper.connect","node001:2181");
        props.put("group.id","test");
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset","latest");

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer<>(
                "test",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(string -> JSON.parseObject(string, Student.class));

        return student;
    }

}
