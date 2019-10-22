import com.xiyun.flink.StreamingT1;
import dao.SourceFromMysql;
import entity.Student;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 数据库
 *
 * @data: 2019/7/20 11:19 AM
 * @author:lixiyan
 */
public class MainMysqlSource {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Student> studentDataStreamSource = env.addSource(new SourceFromMysql());
        SingleOutputStreamOperator<String> map = studentDataStreamSource.map(x -> x.getName());
        map.print();
        env.execute("MainMysqlSource");
    }
}
