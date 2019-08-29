package demo.t1.streaming

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._
/**
  * 不基于窗口，来一个计算一个
  *
  * data  2019/7/21 11:52 AM
  * author lixiyan
  */
object WindowsAllApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text: DataStream[String] = env.socketTextStream("localhost",8888,'\n')
    text.flatMap(_.split("\\s")).map(x=>{
      WordCount(x,1)
    }).keyBy("name").sum("count").print().setParallelism(1)

    env.execute("WindowsAllApp")
  }
  case class WordCount(name:String,count:Int)

}
