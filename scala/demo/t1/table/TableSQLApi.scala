package demo.t1.table

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.{Table, TableEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.types.Row

/**
  * TableSQL的基本用法
  */
object TableSQLApi {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val filePath = "file:///Users/lionli/lixiyan/xflink/Flink_T/scala/demo/t1/resource/people.csv"

    // 已经拿到DataSet
    val csv: DataSet[People] = env.readCsvFile[People](filePath, ignoreFirstLine = false)

    // 拿到table数据
    val people: Table = tableEnv.fromDataSet(csv)

    // 注册成表
    tableEnv.registerTable("people", people)
    // 查找结果数据
    val res: Table = tableEnv.sqlQuery("select * from people")
    // 打印结果数据
    tableEnv.toDataSet[Row](res).print()
  }
  // people类
  case class People(name: String, age: String , job: String)

}
