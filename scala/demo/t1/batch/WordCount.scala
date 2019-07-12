package demo.t1.batch

import demo.t1.resource.WordCountData
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {

    // 获取参数
    val params: ParameterTool = ParameterTool.fromArgs(args)
    // 准备离线环境
    val env = ExecutionEnvironment.getExecutionEnvironment
    // 是参数生效
    env.getConfig.setGlobalJobParameters(params)

    val text =
      if (params.has("input")) {
        env.readTextFile(params.get("input"))
      } else {
        println("Executing wc example with default input data set")
        env.fromCollection(WordCountData.WORDS)
      }
    val counts = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }.map {
      (_, 1)
    }.groupBy(0).sum(1)
    if (params.has("output")){
      counts.writeAsCsv(params.get("output"),"\n"," ")
      env.execute("SCALA WC Example")
    }else{
      counts.print()
    }
//    print("Hello")
  }
}
