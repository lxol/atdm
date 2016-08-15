package sample.stream

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ FileIO, Sink }
//import akka.stream.stage.{ Context, _ }
import akka.util.ByteString
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteOrder
//import sample.stream.Chunker

object Foo {
  def main(args: Array[String]): Unit = {
    import system.dispatcher
    implicit val system = ActorSystem("atdm")
    implicit val materializer = ActorMaterializer()
    //val f1 = new File("/tmp/fifo1")
    val f1 = new File("/tmp/f1.data")
    //val f2 = new File("/tmp/fifo2")
    val f2 = new File("/tmp/f2.data")
    val source1 = FileIO.fromFile(f1, 9).transform(() => new Chunker(9, 1))
    val source2 = FileIO.fromFile(f2, 9).transform(() => new Chunker(9, 2))
    //.runWith(Sink.foreach(println)).onComplete(_ => system.shutdown())
    source1.merge(source2)
      .runWith(FileIO.toFile(new File("/tmp/out.data"), false)).onComplete(_ => system.shutdown())
  }
}

case class Header(streamNumber: Short) {
  implicit val order = ByteOrder.BIG_ENDIAN
  val magic = "MsBaCkUp"

  def encode(): ByteString = {
    val bs = ByteString(magic, "UTF-8")
    val builder = ByteString.newBuilder ++= bs
    builder.putShort(streamNumber).result()
  }
  def decode(bs: ByteString) = {
    ???
  }
}
