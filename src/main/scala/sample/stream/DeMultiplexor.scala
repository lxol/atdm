package sample.stream

import java.io.File

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.io.Framing
import akka.stream.scaladsl._
import akka.stream.stage.{ Context, StatefulStage, SyncDirective }
import akka.util.ByteString

import scala.annotation.tailrec
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ FileIO, Sink }
import akka.stream.stage.{ Context, _ }
import akka.util.ByteString
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteOrder

object DeMultiplexor {

  def main(args: Array[String]): Unit = {
    // actor system and implicit materializer
    implicit val system = ActorSystem("Sys")
    implicit val materializer = ActorMaterializer()

    // execution context

    val LoglevelPattern = """.*\[(DEBUG|INFO|WARN|ERROR)\].*""".r

    // read lines from a log file
    val inputFile = new File("/tmp/output")
    FileIO.fromFile(inputFile, 9).transform(() => new DeChunker(9)).
      map {
        x =>
          {
            val fname = "/tmp/oooo" + x.size + "s" + x.take(1).toString()
            println(fname)
            ///Source(x).runWith(FileIO.toFile(new File(s"$fname")))
          }
      }.
      runWith(Sink.onComplete { _ => system.shutdown() })

    //???

    // FileIO.fromFile(inputFile).
    //   // parse chunks of bytes into lines
    //   via(Framing.delimiter(ByteString(System.lineSeparator), maximumFrameLength = 512, allowTruncation = true)).
    //   map(_.utf8String).
    //   map {
    //     case line @ LoglevelPattern(level) => (level, line)
    //     case line @ other                  => ("OTHER", line)
    //   }.
    //   // group them by log level
    //   groupBy(5, _._1).
    //   fold(("", List.empty[String])) {
    //     case ((_, list), (level, line)) => (level, line :: list)
    //   }.
    //   // write lines of each group to a separate file
    //   mapAsync(parallelism = 5) {
    //     case (level, groupList) =>
    //       Source(groupList.reverse).map(line => ByteString(line + "\n")).runWith(FileIO.toFile(new File(s"target/log-$level.txt")))
    //   }.
    //   mergeSubstreams.
    //   runWith(Sink.onComplete { _ =>
    //     system.shutdown()
    //   })
  }
}

class DeChunker(val chunkSize: Int) extends PushPullStage[ByteString, ByteString] {
  implicit val order = ByteOrder.LITTLE_ENDIAN
  private var buffer = ByteString.empty
  override def onPush(elem: ByteString, ctx: Context[ByteString]): SyncDirective = {
    buffer ++= elem
    emitChunkOrPull(ctx)
  }
  override def onPull(ctx: Context[ByteString]): SyncDirective = emitChunkOrPull(ctx)
  override def onUpstreamFinish(ctx: Context[ByteString]): TerminationDirective =
    if (buffer.nonEmpty) ctx.absorbTermination()
    else ctx.finish()
  private def emitChunkOrPull(ctx: Context[ByteString]): SyncDirective = {
    if (buffer.isEmpty) {
      if (ctx.isFinishing) ctx.finish()
      else ctx.pull()
    } else {
      val (emit, nextBuffer) = buffer.splitAt(chunkSize)
      buffer = nextBuffer
      ctx.push(addHeader(emit))
    }
  }
  def addHeader(bytes: ByteString) = {
    val len = bytes.length
    ByteString.newBuilder.putInt(len).append(bytes).result()
  }
}
