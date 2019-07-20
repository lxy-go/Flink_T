package utils;

import com.alibaba.fastjson.JSON;
import entity.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 向kafka中写入测试数据
 *
 * @data: 2019/7/20 12:11 PM
 * @author:lixiyan
 */
public class KafkaUtils2 {
    public static final String BROKER_LIST = "node001:9092";
    public static final String TOPIC  = "test";

    public static void writeToKafka() throws Exception {
        Properties pros = new Properties();
        pros.put("bootstrap.servers",BROKER_LIST);
        pros.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        pros.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(pros);

        for (int i = 0; i < 100; i++) {
            Student student = new Student(i+100, "lion" + i, "psw" + i, 1);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, null, null, JSON.toJSONString(student));
            producer.send(producerRecord);
            System.out.println("发送数据： "+JSON.toJSONString(student));
        }
        producer.flush();
    }

    public static void main(String[] args) throws Exception{
        writeToKafka();
    }

}
