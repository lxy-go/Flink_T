package utils;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * valueState的类的实现
 *
 * @author lixiyan
 * @data 2019/8/21 10:15 AM
 */
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Long>> {

    /**
     * valueState状态句柄，第一个值为count,第二个值为sum
     */
    private transient ValueState<Tuple2<Long, Long>> sum;



    @Override
    public void open(Configuration config) throws Exception {
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.seconds(10))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                "average",
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {
                }),
                Tuple2.of(0L, 0L));
//        descriptor.enableTimeToLive(ttlConfig);
        sum = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
        // 获取当前状态值
        Tuple2<Long, Long> currentSum = sum.value();

        // 更新
        currentSum.f0 += 1;
        currentSum.f1 += input.f1;

        // 更新状态值
        sum.update(currentSum);
        // 如果count >= 2 清空状态值，重新计算
        if (currentSum.f0 >=2 ){
            out.collect(new Tuple2<>(input.f0,currentSum.f1/currentSum.f0));
            sum.clear();
        }


    }
}
