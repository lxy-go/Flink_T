package demo.t1.source


import java.time.Instant
import java.util.UUID

import com.mysql.jdbc.TimeUtil
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.slf4j.LoggerFactory

import scala.util.Random

/**
  * 源
  *
  * @ date 2019/10/22 2:57 PM
  *
  * @author lixiyan
  */
class StringLineEventSource(val lastTime:Long) extends RichParallelSourceFunction[String]{
  val log = LoggerFactory.getLogger(classOf[StringLineEventSource])

  @volatile private var running = true

  val channelSet = Seq("a","b","c","d")

  val behaviorTypes = Seq("INSTALL", "OPEN", "BROWSE", "CLICK", "PURCHASE", "CLOSE", "UNINSTALL")

  val rand = Random


  override def cancel(): Unit = running =false

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val numElements = Long.MaxValue
    var count =0L

    while(running && count<numElements){
      val channel = channelSet(rand.nextInt(channelSet.size))
      val event = generateEvent();
      log.info("event:[{}]",event)
      val ts = event(0)
      val id = event(1)
      val behaviorType = event(2)
      ctx.collect(Seq(ts,channel,id,behaviorType).mkString("\t"))
      count+=1

    }
  }

  private def generateEvent(): Seq[String] = {
    // simulate 10 seconds lateness
    val ts = Instant.ofEpochMilli(System.currentTimeMillis)
      .minusMillis(lastTime)
      .toEpochMilli

    val id = UUID.randomUUID().toString
    val behaviorType = behaviorTypes(rand.nextInt(behaviorTypes.size))
    // (ts, id, behaviorType)
    Seq(ts.toString, id, behaviorType)
  }
}
