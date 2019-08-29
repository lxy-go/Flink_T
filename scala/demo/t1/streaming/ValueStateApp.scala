package demo.t1.streaming

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.api.scala
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
  * valueState APP
  *
  * @ data 2019/8/21 11:12 AM
  * author lixiyan
  */
object ValueStateApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataSource: DataStream[(String, Int)] = env.fromElements(("1", 3),("1", 3))

    val counts = dataSource
      .keyBy(_._1)
      .mapWithState((in: (String, Int), count: Option[Int]) =>
        count match {
          case Some(c) => ( (in._1, c), Some(c + in._2) )
          case None => ( (in._1, 0), Some(in._2) )
        })

    counts.print()
    env.execute("ValueStateApp")
  }
}
