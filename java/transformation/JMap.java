package transformation;

import entity.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sink.PrintSinkFunction;

/**
 * java版本的map
 * <p>
 * data  2019/7/20 2:04 PM
 * author lixiyan
 */
public class JMap {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Student> dataSource = env.fromElements(
                new Student(1, "a", "xxx", 10),
                new Student(1, "b", "xxx", 11),
                new Student(1, "c", "xxx", 12)
        );
        SingleOutputStreamOperator<Student> data = dataSource.map(x -> {
            return new Student(x.getId(), x.getName() + 1, x.getPassword() + "*", x.getAge() + 10);
        });
        data.addSink(new PrintSinkFunction<>());
        env.execute("JMap");
    }
}
