package demo.t1.streaming

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
object FileSystemSinkApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[String] = env.readTextFile("file:///Users/lionli/lixiyan/xflink/Flink_T/scala/demo/t1/resource/people.csv")
    text.print()
    env.execute("FileSystemSinkApp")

  }

}
