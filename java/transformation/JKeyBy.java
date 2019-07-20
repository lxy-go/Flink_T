package transformation;

import entity.Student;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.KafkaSource;

/**
 * java keyBy
 * <p>
 * data  2019/7/20 4:09 PM
 * author lixiyan
 */
public class JKeyBy {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator stu = KafkaSource.getObjectConsume(env);

        stu.keyBy(new KeySelector<Student,Integer>(){

            @Override
            public Integer getKey(Student value) throws Exception {
                return value.getAge();
            }
        }).print().setParallelism(1);
        env.execute("JKeyBy");
    }
}
