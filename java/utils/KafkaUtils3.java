package utils;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;


/**
 * 模拟产生日志数据
 * <p>
 * data  2019/7/22 10:09 PM
 * author lixiyan
 */
public class KafkaUtils3 {
    public static void main(String[] args) throws Exception{
        Properties props = new Properties();

        props.setProperty("bootstrap.servers","node001:9092");
        props.setProperty("key.serializer",StringSerializer.class.getName());
        props.setProperty("value.serializer",StringSerializer.class.getName());
        String topic = "test";
        KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);

        while (true){
            StringBuilder sb = new StringBuilder();

            sb.append("imooc").append("\t");
            sb.append("CN").append("\t");
            sb.append(getLevel()).append("\t");
            sb.append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("\t");
            sb.append(getIps()).append("\t");
            sb.append(getDomain()).append("\t");
            sb.append(getTraffic()).append("\t");
            System.out.println(sb.toString());
            producer.send(new ProducerRecord<>(topic,sb.toString()));
            Thread.sleep(2000);
        }
    }

    public static long getTraffic(){
        return new Random().nextInt(10000);

    }

    public static String getLevel(){
        String[] levels = {"M", "E"};
        return levels[new Random().nextInt(levels.length)];
    }

    public static String getIps(){
        String[] ips = new String[]{
                "223.104.18.110",
                "113.101.75.194",
                "27.17.127.135",
                "183.225.139.16",
                "112.1.66.34",
                "175.148.211.190",
                "183.227.58.21",
                "59.83.198.84",
                "117.28.38.28",
                "117.59.39.169"
        };
        return ips[new Random().nextInt(ips.length)];
    }

    public static String getDomain(){
        String[] domains = new String[]{
                "v1.go2yd.com",
                "v2.go2yd.com",
                "v3.go2yd.com",
                "v4.go2yd.com",
                "vmi.go2yd.com"
        };

        return domains[new Random().nextInt(domains.length)];
    }
}
