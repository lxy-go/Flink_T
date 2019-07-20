package transformation;

import com.alibaba.fastjson.JSON;
import entity.Student;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import sink.MysqlSink;
import utils.KafkaSource;

import java.util.Properties;


/**
 * java版本的FlatMap
 * <p>
 * data  2019/7/20 3:12 PM
 * author lixiyan
 */
public class JFlatMap {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator student = KafkaSource.getObjectConsume(env);

        SingleOutputStreamOperator<Student> flatMap = student.flatMap(new FlatMapFunction<Student, Student>() {
            @Override
            public void flatMap(Student student, Collector<Student> out) throws Exception {
                if (student.getId() % 2 == 0) {
                    out.collect(student);
                }
            }
        });
        flatMap.print();

        env.execute("JFlatMap");
    }
}
