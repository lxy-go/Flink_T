package demo.t1

import java.util.Properties

import demo.t1.source.StringLineEventSource
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

/**
  * TODO
  *
  * @ date 2019/10/22 3:28 PM
  *
  * @author lixiyan
  */
object MainWaterMarkSource {
  def main(args: Array[String]): Unit = {
    val sourceLatenessMillis =120000
    val windowSizeMillis = 50000
    val maxLaggedTimeMillis =60000
    val TOPIC = "waterMark"

    val props = new Properties()
    props.setProperty("bootstrap.servers", "node001:9092")
    props.setProperty("group.id","test")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream = env.addSource(new StringLineEventSource(sourceLatenessMillis))

    stream.setParallelism(1).assignTimestampsAndWatermarks(
      // 指派时间戳
      new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(maxLaggedTimeMillis)) {
        override def extractTimestamp(element: String): Long = {
          return element.split("\t")(0).toLong
        }
      }
    ).map(line=>{
      val a = line.split("\t")
      val channel = a(1)
      ((channel, a(3)), 1L)
    }).keyBy(0).window(TumblingEventTimeWindows.of(Time.milliseconds(windowSizeMillis))).sum("a")
//      .map(t => {
//      val windowStart = t._1
//      val windowEnd = t._2
//      val channel = t._3
//      val behaviorType = t._4
//      val count = t._5
//      Seq(windowStart, windowEnd, channel, behaviorType, count).mkString("\t")
//    }).print()

    env.execute("MainWaterMarkSource")

  }
}
