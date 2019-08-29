package batch;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 分布式缓存
 *
 * @author lixiyan
 * @data 2019/8/27 10:01 AM
 */
public class DistributeCache {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        env.registerCachedFile("/Users/lionli/lixiyan/xflink/Flink_T/java/batch/resource/a.txt","a.txt");

        DataSource<String> data = env.fromElements("a", "b", "c", "d");
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            private ArrayList<String> dataList = new ArrayList<String>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File myFile = getRuntimeContext().getDistributedCache().getFile("a.txt");
                List<String> lines = FileUtils.readLines(myFile);
                for (String line : lines) {
                    this.dataList.add(line);
                    System.out.println("分布式缓存为："+line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                // 使用dataList
                System.out.println("使用dataList: "+dataList+"-----"+value);
                // 业务逻辑
                return dataList+": "+value;
            }
        });
        result.print();
    }
}
