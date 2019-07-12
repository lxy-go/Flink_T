package demo.t1.batch
import org.apache.flink.api.common.accumulators.LongCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * 计数器
  * 1.定义计数器
  * 2.注册计数器
  * 3.获取计数器
  */
object CounterApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = env.fromElements("Hadoop","Spark","Flink","Python")
    val info = data.map(new RichMapFunction[String,String]() {
      // step1:定义计数器
      val counter = new LongCounter()


      override def open(parameters: Configuration): Unit = {
        // step2:注册计数器
        getRuntimeContext.addAccumulator("xiyan",counter)
      }

      override def map(in: String): String = {
        counter.add(1)
        in
      }
    })
    val filePath = "file:///Users/lionli/lixiyan/xflink/Flink_T/scala/demo/t1/batch/resource/count.txt"
    info.writeAsText(filePath,WriteMode.OVERWRITE).setParallelism(1)
    val jobResult = env.execute("CounterApp")

    // step3:获取计数器
    val num = jobResult.getAccumulatorResult[Long]("xiyan")
    println("num: "+num)
  }
}
