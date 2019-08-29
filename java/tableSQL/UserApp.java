package tableSQL;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * 这就主函数，负责统计，引用是java的，别引错了
 */
public class UserApp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<UserInfo> userInfoDataStream = env.addSource(new UserDataSource());

        userInfoDataStream.print();
        DataStream<UserInfo> timedData = userInfoDataStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserInfo>() {
            @Override
            public long extractAscendingTimestamp(UserInfo element) {
                return element.getPTime().getTime();
            }
        });
        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        // pTime.rowtime  = pTime as rowTime(proctime)
        tableEnv.registerDataStream("test", timedData, "userId,itemId,pTime.rowtime");
        Table result = tableEnv.sqlQuery("SELECT userId,TUMBLE_END(pTime, INTERVAL '5' SECOND) as pTime,count(1) as cnt FROM  test" +
                " GROUP BY TUMBLE(pTime, INTERVAL '5' SECOND),userId ");
        // deal with (Tuple2<Boolean, Row> value) -> out.collect(row)
        SingleOutputStreamOperator allClick = tableEnv.toRetractStream(result, Row.class)
                .flatMap((Tuple2<Boolean, Row> value, Collector<Row> out) -> {
                    out.collect(value.f1);
                }).returns(Row.class);
        // add sink or print
//        allClick.print();
        env.execute("test");
    }

}