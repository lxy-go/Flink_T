package source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * source输出
 *
 * @author lixiyan
 * @data 2019/8/27 1:39 PM
 */
public class MyNoParalleSource implements SourceFunction<String> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while(isRunning){
            // 图书排行
            List<String> books = new ArrayList<>();
            books.add("Python入门到放弃");
            books.add("Java入门到放弃");
            books.add("Scala入门到放弃");
            books.add("C++入门到放弃");
            books.add("PHP入门到放弃");
            int i = new Random().nextInt(5);
            ctx.collect(books.get(i));
            Thread.sleep(1000);
        }

    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
