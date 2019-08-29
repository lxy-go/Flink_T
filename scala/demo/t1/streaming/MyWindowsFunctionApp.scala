package demo.t1.streaming

import demo.t1.utils.MyWindowFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
  * 自定义windows函数的主类
  *
  * data  2019/7/21 12:21 PM
  * author lixiyan
  */
object MyWindowsFunctionApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost",8888,'\n')

//    text.flatMap(_.split("\\s")).map(x=>Wc(x,1)).keyBy("word").window(TumblingProcessingTimeWindows.of(Time.seconds(5))).reduce(new MyWindowFunction)

    env.execute("MyWindowsFunctionApp")
  }
  case class Wc(word:String,count:Int)
}
