package demo.t1.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

object DataStreamSourceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    nonParallelSourceFunction(env)
    env.execute("DataStreamSourceApp")
  }

  def nonParallelSourceFunction(env:StreamExecutionEnvironment): Unit ={
    val data = env.addSource(new CustomNonParallelSourceFunction).setParallelism(1)
    data.print().setParallelism(1)
  }
}
