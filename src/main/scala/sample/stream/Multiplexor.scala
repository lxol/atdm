package sample.stream

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ FileIO, Sink }
import akka.util.ByteIterator
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
    val source1 = FileIO.fromFile(f1, Frame.payloadSize).transform(() => new Chunker(Frame.payloadSize, 1))
    val source2 = FileIO.fromFile(f2, Frame.payloadSize).transform(() => new Chunker(Frame.payloadSize, 2))
    source1.merge(source2)
      .runWith(FileIO.toFile(new File("/tmp/out.data"), false)).onComplete(_ => system.shutdown())
  }
}

case class Header(streamNumber: Short,
    eof: Short, invalids: Int) {
  implicit val order = ByteOrder.BIG_ENDIAN
  val magic = Header.magic
  val padding = Header.padding
  //val padding = new Array[Byte](2)

  def encode(): ByteString = {
    val bs = ByteString(magic, "UTF-8")
    val builder = ByteString.newBuilder ++= bs
    builder.putShort(streamNumber).
      putShort(eof).
      putInt(invalids).
      putBytes(padding).result()
  }
}

object Header {
  implicit val order = ByteOrder.BIG_ENDIAN

  val size = 512
  val magic = "MsBaCkUp"
  val padding = new Array[Byte](size - 16)
  def apply(bs: ByteString) = decode(bs)

  def decode(bs: ByteString): Header = {
    val iter = bs.iterator
    val bytes = new Array[Byte](8)
    iter getBytes (bytes)
    val magicDecoded = ByteString(bytes).utf8String
    if (magic != magicDecoded) throw new Exception(s"bad header magic string ${magicDecoded}")
    val streamNumberDecoded = iter.getShort
    val eofDecoded = iter.getShort
    val invalidsDecoded = iter.getInt
    Header(streamNumberDecoded, eofDecoded, invalidsDecoded)
  }

}

object Frame {
  val size = 1024
  val payloadSize = size - Header.size
}
