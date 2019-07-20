package utils;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Properties;

/**
 * 定时循环写metrics信息到kafka
 *
 * @data: 2019/7/19 9:23 PM
 * @author:lixiyan
 */
public class KafkaUtils {
    public static final String broker_list = "node001:9092";

    public static final String topic = "test";

    public static void writeToKafka() throws Exception {
        Properties props = new Properties();

        props.put("bootstrap.servers",broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Metric metric = new Metric();

        metric.setTimestampe(System.currentTimeMillis());
        metric.setName("mem");
        HashMap<String, String> tags = new HashMap<>();
        HashMap<String, Object> fields = new HashMap<>();

        tags.put("cluster","xygj");
        tags.put("hostname","node001");

        fields.put("cpuLoad",0.12);
        fields.put("max",999L);
        fields.put("min",667L);
        fields.put("cpuCores",12);

        metric.setTags(tags);
        metric.setFields(fields);
        ProducerRecord record = new ProducerRecord<String,String>(topic,null,null, JSON.toJSONString(metric));
        producer.send(record);

        System.out.println("发送数据： "+JSON.toJSONString(metric));
        producer.flush();
    }

    public static void main(String[] args) throws Exception {
        while (true){
            Thread.sleep(3000);
            writeToKafka();
        }
    }
}
