package transformation

import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import transformation.TMap.Student
import org.apache.flink.api.scala._

/**
  * scala flatMap
  *
  * data  2019/7/20 2:10 PM
  * author lixiyan
  */
object TFlatMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.addSource(new PublicSource2)
    data.flatMap(_.split("\t")).map((_, 1)).keyBy(0).timeWindow(Time.seconds(5)).sum(1).print().setParallelism(1)
    env.execute("TFlatMap")
  }
}
