package stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.scala.function.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;

import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * TopN案例：图书统计
 * 来源:Kafka
 * 输出：print
 *
 * @author lixiyan
 * @data 2019/8/27 1:37 PM
 */
public class TopNApp {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","172.16.206.133:9092");
        FlinkKafkaConsumer<String> input = new FlinkKafkaConsumer<>("test1", new SimpleStringSchema(), properties);

        DataStream<String> stream = env.addSource(input).setParallelism(1);
        DataStream<Tuple2<String, Integer>> ds = stream.flatMap(new LineSplitter()).setParallelism(1);
        DataStream<Tuple2<String, Integer>> wcount = ds.keyBy(0).window(SlidingProcessingTimeWindows.of(Time.seconds(60),Time.seconds(5))).sum(1).setParallelism(1);
//        wcount.print();

        wcount.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5))).process(new TopNAllFunction(3)).print();


        env.execute("TopNApp");
    }
    private static final class LineSplitter implements FlatMapFunction<String,Tuple2<String,Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<String, Integer>(value,1));
        }
    }

    private static class TopNAllFunction extends ProcessAllWindowFunction<Tuple2<String,Integer>,String,TimeWindow> {
        private int topSize = 3;
        public TopNAllFunction(int topSize){
            this.topSize = topSize;
        }


        @Override
        public void process(Context context, java.lang.Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
            TreeMap<Integer, Tuple2<String, Integer>> treemap = new TreeMap<Integer, Tuple2<String, Integer>>(
                    new Comparator<Integer>() {

                        @Override
                        public int compare(Integer y, Integer x) {
                            return (x < y) ? -1 : 1;
                        }

                    }); //treemap按照key降序排列，相同count值不覆盖

            for (Tuple2<String, Integer> element : input) {
                treemap.put(element.f1, element);
                if (treemap.size() > topSize) { //只保留前面TopN个元素
                    treemap.pollLastEntry();
                }
            }


            for (Map.Entry<Integer, Tuple2<String, Integer>> entry : treemap
                    .entrySet()) {
                out.collect("=================\n热销图书列表:\n"+ new Timestamp(System.currentTimeMillis()) + treemap.toString() + "\n===============\n");
            }
        }
    }
}

