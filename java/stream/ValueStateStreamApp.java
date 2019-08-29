package stream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import utils.CountWindowAverage;

/**
 * valueState的例子
 *
 * @author lixiyan
 * @data 2019/8/21 10:15 AM
 */
public class ValueStateStreamApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.fromElements(Tuple2.of(1L,3L),Tuple2.of(1L,5L),Tuple2.of(1L,7L),Tuple2.of(1L,7L),Tuple2.of(2L,2L),Tuple2.of(2L,2L))
                .keyBy(0)
                .flatMap(new CountWindowAverage())
                .print();
        env.execute("ValueStateStreamApp");
    }
}
