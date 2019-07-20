package transformation

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import transformation.TMap.Student

/**
  * TODO
  *
  * data  2019/7/20 2:12 PM
  * author lixiyan
  */
class PublicSource extends RichSourceFunction[Student]{

  var out = new Student("a",1);
  var i =0

  var isRunning = true

  override def cancel(): Unit = {
    isRunning = false
  }

  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    while(isRunning) {
      ctx.collect(out)
      if (i>10){
        i =0
      }
      i+=1
      out = new Student("a"+i,i)
      Thread.sleep(1000)
    }
  }
}
