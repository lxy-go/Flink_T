package sink;

import entity.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 写到Mysql的Sink
 *
 * @data: 2019/7/20 12:54 PM
 * @author:lixiyan
 */
public class MysqlSink extends RichSinkFunction<Student>{
    PreparedStatement ps;
    private Connection conn;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = getConnection();
        String sql = "insert into student(id, name, password, age) values(?, ?, ?, ?);";
        ps = this.conn.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (conn != null){
            conn.close();
        }
        if (ps != null){
            ps.close();
        }
    }

    @Override
    public void invoke(Student value, Context context) throws Exception {
        ps.setInt(1,value.getId());
        ps.setString(2,value.getName());
        ps.setString(3,value.getPassword());
        ps.setInt(4,value.getAge());
        ps.executeUpdate();
    }

    private static Connection getConnection(){
        Connection conn = null;
        try{
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink", "root", "root");
        }catch (Exception e){
            System.out.println("数据库连接失败  "+e.getMessage());
        }
        return conn;
    }
}
