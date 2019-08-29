package demo.t1.utils



import java.lang

import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


/**
  * 自定义WindowsFunction
  *
  * data  2019/7/21 12:16 PM
  * author lixiyan
  */
class MyWindowFunction extends WindowFunction[(String, Long), String, String, TimeWindow] {

  override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
    var count = 0L
    for (in <- input) {
      count = count + 1
    }
    out.collect(s"Window $window count: $count")
  }
}