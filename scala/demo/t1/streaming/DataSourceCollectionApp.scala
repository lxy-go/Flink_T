package demo.t1.streaming

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * TODO
  *
  * @author lixiyan
  */
object DataSourceCollectionApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements(
      new Person("a"),
      new Person("b"),
      new Person("c")
    )
    data.print()
    env.execute("DataSourceCollectionApp")
  }
  case class Person(name:String)

}
