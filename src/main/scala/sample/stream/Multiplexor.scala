package sampele.stream
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ FileIO, Sink }
import akka.stream.stage.{ Context, _ }
import akka.util.ByteString
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteOrder

object Foo {
  def main(args: Array[String]): Unit = {
    import system.dispatcher
    implicit val system = ActorSystem("atdm")
    implicit val materializer = ActorMaterializer()
    val f1 = new File("/tmp/fifo1")
    //val f1 = new File("/tmp/f1.data")
    val f2 = new File("/tmp/fifo2")
    //val f2 = new File("/tmp/f2.data")
    val source1 = FileIO.fromFile(f1, 9).transform(() => new Chunker(9))
    val source2 = FileIO.fromFile(f2, 9).transform(() => new Chunker(9))
    //.runWith(Sink.foreach(println)).onComplete(_ => system.shutdown())
    source1.merge(source2)
      .runWith(FileIO.toFile(new File("/tmp/out.data"), false)).onComplete(_ => system.shutdown())
  }
}

class Chunker(val chunkSize: Int) extends PushPullStage[ByteString, ByteString] {
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
//val chunksStream = rawBytes.transform(() => new Chunker(ChunkLimit))
