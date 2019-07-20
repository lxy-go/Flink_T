package transformation;

import entity.Student;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.KafkaSource;

/**
 * java的filter
 * <p>
 * data  2019/7/20 4:01 PM
 * author lixiyan
 */
public class JFilter {
    public static void main(String[] args)throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator student = KafkaSource.getObjectConsume(env);
        student.filter(new FilterFunction<Student>(){

            @Override
            public boolean filter(Student student) throws Exception {
                if (student.getId()>60){
                    return true;
                }
                return false;
            }
        }).print();

        env.execute("JFilter");
    }
}
