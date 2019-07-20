package transformation

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
/**
  * map算子
  *
  * data  2019/7/20 1:52 PM
  * author lixiyan
  */
object TMap {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    // 模拟数据
//    val dataSource = env.fromElements(
//      new Student("a", 1),
//      new Student("b", 2),
//      new Student("c", 3)
//    )
    val dataSource = env.addSource(new PublicSource)
    dataSource.map(x=>{
      new Student(x.name+"1",x.age+2)
    }).print()
//    dataSource.print()
    env.execute("JMap")

  }

  case class Student(name:String,age:Int)
}
