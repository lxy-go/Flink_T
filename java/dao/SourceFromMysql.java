package dao;

import entity.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * MYSQL作为flink的source
 *
 * @data: 2019/7/20 11:01 AM
 * @author:lixiyan
 */
public class SourceFromMysql extends RichSourceFunction<Student>{

    PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "select * from student";
        ps= this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null){
            connection.close();
        }if (ps != null){
            ps.close();
        }
    }

    @Override
    public void run(SourceContext<Student> ctx) throws Exception {
        ResultSet res = ps.executeQuery();
        while (res.next()){
            Student stu = new Student();
            stu.setId(res.getInt("id"));
            stu.setName(res.getString("name"));
            stu.setPassword(res.getString("password"));
            stu.setAge(res.getInt("age"));
            ctx.collect(stu);
        }


    }

    @Override
    public void cancel() {

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
