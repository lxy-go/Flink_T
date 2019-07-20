package sink;

import lombok.Data;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;

import java.io.PrintStream;

/**
 * TODO
 *
 * @data: 2019/7/20 11:45 AM
 * @author:lixiyan
 */
@PublicEvolving
@Data
public class PrintSinkFunction<IN> extends RichSinkFunction<IN> {
    private static final long serialVersionUID = 1L;

    private static final boolean STD_OUT= false;

    private static final boolean STD_ERR = true;
    private boolean target;
    private transient PrintStream stream;
    private transient String prefix;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();

        stream = target == STD_OUT? System.out:System.err;

        prefix = (context.getNumberOfParallelSubtasks() > 1) ?
                ((context.getIndexOfThisSubtask() + 1) + "> ") : null;
    }

    @Override
    public void invoke(IN record) throws Exception {
        if (prefix != null) {
            stream.println(prefix+"-----自定义sink------" + record.toString());
        }
        else {
            stream.println(record.toString());
        }

    }

    @Override
    public void close() throws Exception {
        this.stream = null;
        this.prefix = null;
    }
}
