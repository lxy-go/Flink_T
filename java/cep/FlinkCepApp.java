package cep;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

/**
 * Flink 的cep范例
 *
 * @author lixiyan
 * @data 2019/8/29 4:10 PM
 */
public class FlinkCepApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream("localhost", 12345, '\n');

        DataStream<PowerEvent> out = text.flatMap(new FlatMapFunction<String, PowerEvent>() {
            @Override
            public void flatMap(String input, Collector<PowerEvent> out) throws Exception {
                String[] words = input.split("\\s");
                for (String word : words) {
                    PowerEvent powerEvent = new PowerEvent(word);
                    out.collect(powerEvent);
                }
            }
        });
        // a的下一输入是b
        Pattern<PowerEvent, PowerEvent> parttern = Pattern.<PowerEvent>begin("First")
                .subtype(PowerEvent.class).where(new SimpleCondition<PowerEvent>() {
            @Override
            public boolean filter(PowerEvent value) throws Exception {
                return value.getVoltage().equals("a");
            }
        }).next("second").subtype(PowerEvent.class).where(new SimpleCondition<PowerEvent>() {
                    @Override
                    public boolean filter(PowerEvent value) throws Exception {
                        return value.getVoltage().equals("b");
                    }
                });

        PatternStream<PowerEvent> patternStream = CEP.pattern(out, parttern);

        DataStream<String> res = patternStream.select(new PatternSelectFunction<PowerEvent, String>() {
            @Override
            public String select(Map<String, List<PowerEvent>> map) throws Exception {

                return "你输入了a b";
            }
        });
        res.print();
        env.execute("FlinkCepApp");
    }


}
