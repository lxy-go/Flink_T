package tableSQL;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * 模拟数据产生，没隔1秒发送一个数据
 * @author <a href="mailto:huoguo@2dfire.com">火锅</a>
 * @time 2019/2/22
 */
public class UserDataSource implements SourceFunction<UserInfo> {
    static String[] items = {"i-1", "i-2", "i-3"};
    static String[] users = {"a", "b", "c"};
    @Override
    public void run(SourceContext sc) throws Exception {
        while (true) {
            TimeUnit.SECONDS.sleep(1);
            int m = (int) (System.currentTimeMillis() % 3);
            sc.collect(new UserInfo(users[m], items[m]));
        }
    }
    @Override
    public void cancel() {
        System.out.println("cancel to do ...");
    }
}
