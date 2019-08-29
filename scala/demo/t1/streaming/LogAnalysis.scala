package demo.t1.streaming

import java.text.SimpleDateFormat
import java.util.Properties

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

//import org.apache.commons.math3.analysis.function.Max
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.slf4j.LoggerFactory
/**
  * scala日志分析
  *
  * data  2019/7/22 10:29 PM
  * author lixiyan
  */
object LogAnalysis {
  def main(args: Array[String]): Unit = {

    val log = LoggerFactory.getLogger("LogAnalysis")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val topic = "test"
    val props = new Properties()
    props.setProperty("bootstrap.servers","node001:9092")
    props.setProperty("group.id","test")

    val kafkaSource = new FlinkKafkaConsumer(topic,new SimpleStringSchema(),props)
    val data = env.addSource(kafkaSource)
    val logData = data.map(x => {
      val splits = x.split("\t");
      val level = splits(2)
      var timestampe = 0l
      val time = splits(3)
      try{
        val sourceFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        timestampe = sourceFormat.parse(time).getTime
      }catch {
        case e:Exception=>{
          log.warn("e"+e)
        }
      }
      val domain = splits(5)
      val traffic = splits(6).toLong
      (level, timestampe, domain, traffic)
    }).filter(_._2 != 0).filter(_._1 == "E")
      .map(x => {
        (x._2, x._3, x._4)   // 1 level(抛弃)  2 time  3 domain   4 traffic
      })

    val resultData = logData.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, String, Long)] {
      val maxOutofOrderness = 10000l // 水位的超时时间
      var currentMaxTimestamp:Long = _


      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutofOrderness)
      }

      override def extractTimestamp(element: (Long, String, Long), previousElementTimestamp: Long): Long = {
        val timestampe = element._1
        currentMaxTimestamp = Math.max(currentMaxTimestamp,timestampe)
        timestampe
      }
    }).keyBy(1).window(TumblingEventTimeWindows.of(Time.seconds(60)))
      .apply(new WindowFunction[(Long,String,Long),(String,String,Long),Tuple,TimeWindow] {

        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(Long, String, Long)], out: Collector[(String, String, Long)]): Unit = {
          val domain = key.getField(0).toString
          var sum = 0l
          val times = ArrayBuffer[Long]()

          val iterator = input.iterator

          while (iterator.hasNext){
            val next = iterator.next()
            sum += next._3 //traffic求和

            times.append(next._1)
          }

          val time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(times.max)

          /**
            * 1. time
            * 2. domain
            * 3. traffic
            */
          out.collect((time,domain,sum))

        }
      })

    resultData.print()
    env.execute("LogAnalysis")
  }
}
