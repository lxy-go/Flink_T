package demo.t1.table

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row


/**
  * TODO
  *
  * data 2019/8/5 1:32 PM
  * author lixiyan
  */
object ObjectPvUvSQL {

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    val input = env.fromElements(
      PageVisit("2017-09-16 09:00:00", 1001, "/page1"),
      PageVisit("2017-09-16 09:00:00", 1001, "/page2"),
      PageVisit("2017-09-16 10:30:00", 1005, "/page1"),
      PageVisit("2017-09-16 10:30:00", 1005, "/page1"),
      PageVisit("2017-09-16 10:30:00", 1005, "/page2"))

    // register the DataStream as table "visit_table"
    tEnv.registerDataStream("visit_table", input, 'visit_time, 'user_id, 'visit_page)

    // run a SQL query on the Table
    val table = tEnv.sqlQuery(
      "SELECT " +
        "  visit_time, " +
        "  DATE_FORMAT(max(visit_time), 'HH') as ts, " +
        "  count(user_id) as pv, " +
        "  count(distinct user_id) as uv " +
        "FROM visit_table " +
        "GROUP BY visit_time")

    val dataStream = tEnv.toRetractStream[Row](table)

    if (params.has("output")) {
      val outPath = params.get("output")
      System.out.println("Output path: " + outPath)
      dataStream.writeAsCsv(outPath)
    } else {
      System.out.println("Printing result to stdout. Use --output to specify output path.")
      dataStream.print()
    }
    env.execute()
  }

  // *************************************************************************
  //     USER DATA TYPES
  // *************************************************************************

  case class PageVisit(visit_time: String, user_id: Long, visit_page: String)

}
