package broadcaststate;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * 广播变量
 *
 * @author lixiyan
 * @data 2019/8/27 11:08 AM
 */
public class BroadCastTestApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 1.封装一个DataSet
        DataSource<Integer> broadCast = env.fromElements(1, 2, 3);

        DataSource<String> data = env.fromElements("a", "b");

        data.map(new RichMapFunction<String, String>() {
            List list = new ArrayList();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                List<Integer> number = getRuntimeContext().getBroadcastVariable("number");
                list.addAll(number);
            }

            @Override
            public String map(String value) throws Exception {

                return value+": "+list;
            }
        }).withBroadcastSet(broadCast,"number").print();



    }
}
