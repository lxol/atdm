package sample.stream

import akka.stream.Supervision
import java.io.{ File, FileOutputStream }

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
import akka.util.ByteString
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteOrder

object DeMultiplexor {

  def main(args: Array[String]): Unit = {
    // actor system and implicit materializer
    implicit val system = ActorSystem("Sys")
    implicit val materializer = ActorMaterializer()
    val inputFile = new File("/tmp/out.data")
    FileIO.fromFile(inputFile, 512 + 9). //transform(() => new DeChunker(512 + 9)).
      map {
        bs =>
          {
            val (headerByteString, payload) = bs.splitAt(512)
            val header = Header(headerByteString)
            val os = new FileOutputStream(s"/tmp/result${header.streamNumber}", true)
            os.write(payload.toArray, 0, 9 - header.invalids)
          }
      }.
      runWith(Sink.onComplete { _ => system.shutdown() })
  }
}
