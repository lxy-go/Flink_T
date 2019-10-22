
import entity.Student;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

/**
 * 两个源JOIN
 *
 * @author lixiyan
 * @date 2019/10/22 9:48 AM
 */
public class MainTwoSourceApp {
    public static Map map = new HashMap();
    public static Connection connection;
    public static PreparedStatement ps;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String TOPIC = "twoSourceTopic";
        Properties props = new Properties();
        props.put("bootstrap.servers", "node001:9092");
        props.put("zookeeper.connect", "node001:2181");
        props.put("group.id", "test1");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        FlinkKafkaConsumer kafkaSource = new FlinkKafkaConsumer<String>(TOPIC, new SimpleStringSchema(), props);

        DataStreamSource dataStreamSource = env.addSource(kafkaSource);

        dataStreamSource.map(new RichMapFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                connection = getConnection();
                String sql = "select * from student";
                ps = connection.prepareStatement(sql);
                System.out.println("open-------");

            }

            @Override
            public String map(String value) throws Exception {
                System.out.println("map-------");
                ResultSet res = ps.executeQuery();
                while (res.next()) {
                    Student stu = new Student();
                    stu.setId(res.getInt("id"));
                    stu.setName(res.getString("name"));
                    stu.setPassword(res.getString("password"));
                    stu.setAge(res.getInt("age"));
                    System.out.println(stu.toString());
                    if (value.equals(stu.getName())){
                        return stu.toString();
                    }
                }

                return "ccc";
            }
        }).print();


        env.execute("MainMysqlSource");
    }

    private static Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink", "root", "root");
        } catch (Exception e) {
            System.out.println("数据库连接失败  " + e.getMessage());
        }
        return conn;
    }
}
