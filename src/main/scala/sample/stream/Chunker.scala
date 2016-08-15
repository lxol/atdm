package sample.stream
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ FileIO, Sink }
import akka.stream.stage.{ Context, _ }
import akka.util.ByteString
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteOrder

class Chunker(val chunkSize: Int, val streamNum: Short) extends PushPullStage[ByteString, ByteString] {
  implicit val order = ByteOrder.BIG_ENDIAN
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
    //val len = bytes.length
    val header = Header(streamNum)
    val builder = ByteString.newBuilder ++= header.encode()
    builder.append(bytes).result()
  }

}
