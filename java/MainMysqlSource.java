import com.xiyun.flink.StreamingT1;
import dao.SourceFromMysql;
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
        env.addSource(new SourceFromMysql()).print();
        env.execute("MainMysqlSource");
    }
}
