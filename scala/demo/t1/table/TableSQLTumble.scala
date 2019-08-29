package demo.t1.table

import java.sql.Timestamp

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
/**
  * 带时间窗口的sql
  *
  * data 2019/8/5 1:10 PM
  * @ author lixiyan
  */
object TableSQLTumble {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val text: DataStream[String] = env.socketTextStream("localhost", 12345,'\n')
    val wc: DataStream[WordCount] = text.flatMap(_.split("\\s")).map(WordCount(_,1L,new Timestamp(System.currentTimeMillis())))
    val wordcount: DataStream[WordCount] = wc.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[WordCount] {
      val maxOutofOrderness = 100l // 水位的超时时间
      var currentMaxTimestamp: Long = _

      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutofOrderness)
      }

      override def extractTimestamp(element: WordCount, previousElementTimestamp: Long): Long = {
        val timestampe = element.ptime.getTime
        currentMaxTimestamp = Math.max(currentMaxTimestamp, timestampe)
        timestampe
      }
    })

    val talEnv: scala.StreamTableEnvironment = TableEnvironment.getTableEnvironment(env)

//    wc.print()

//    tableEnv.registerDataStream("Orders", ds, 'user, 'product, 'amount)
    talEnv.registerDataStream("wordcount",wordcount,'word,'count, 'ptime.rowtime)
    val sql =
      """
        |select word,TUMBLE_END(ptime, INTERVAL '5' SECOND) as pTime
        |from wordcount GROUP BY TUMBLE(ptime, INTERVAL '5' SECOND),word
      """.stripMargin

   val table = talEnv.sqlQuery(sql)
    val value: DataStream[(Boolean, Row)] = talEnv.toRetractStream[Row](table)
    value.print()

    env.execute("TableSQLTumble run")
  }

  case class WordCount(word:String,count:Long,ptime:Timestamp)

}
