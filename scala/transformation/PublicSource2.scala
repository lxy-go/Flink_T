package transformation

import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import transformation.TMap.Student

/**
  * TODO
  *
  * data  2019/7/20 2:12 PM
  * author lixiyan
  */
class PublicSource2 extends RichSourceFunction[String]{

  var out = "";
  var i =1

  var isRunning = true

  override def cancel(): Unit = {
    isRunning = false
  }

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while(isRunning) {
      ctx.collect(out)
      if (i>9){
        i =0
      }
      i+=1
      out += i+""+"\t"
      Thread.sleep(1000)
    }
  }
}
