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
  private var isFinished = false

  override def onPush(elem: ByteString, ctx: Context[ByteString]): SyncDirective = {
    buffer ++= elem
    emitChunkOrPull(ctx)
  }

  override def onPull(ctx: Context[ByteString]): SyncDirective = emitChunkOrPull(ctx)

  override def onUpstreamFinish(ctx: Context[ByteString]): TerminationDirective =
    if (buffer.nonEmpty) ctx.absorbTermination()
    else ctx.finish()

  private def emitChunkOrPull(ctx: Context[ByteString]): SyncDirective = {
    // val bufferEmpty = buffer.isEmpty
    // val isFinishing = ctx.isFinishing
    // val smallBuffer = buffer.length < chunkSize
    // val sizeOfchunk = buffer.length == chunkSize

    // val bl = buffer.length
    // isFinishing match {
    //   case true if bl == 0         => ctx.finish()
    //   case true if bl <= chunkSize => ctx.push(addHeader(buffer, 1))
    //   case true if bl > chunkSize => {
    //     val (emit, nextBuffer) = buffer.splitAt(chunkSize)
    //     buffer = nextBuffer
    //     ctx.push(addHeader(emit, 1))
    //   }
    //   case false if bl < chunkSize => ctx.pull()
    //   case false if bl >= chunkSize => {
    //     val (emit, nextBuffer) = buffer.splitAt(chunkSize)
    //     buffer = nextBuffer
    //     ctx.push(addHeader(emit, 0))
    //   }
    // }

    // (bufferEmpty, smallBuffer, isFinishing) match {

    //   case (true , _, true) => ctx.finish()
    //   case (true , _, false) => ctx.pull()
    //   case (_, true, false) => ctx.pull()
    //   case (false, true , true) => ctx.push(addHeader(buffer, 1))
    //   case (false, true , false) => ctx.push(addHeader(buffer, 1))

    //   case _ => ???
    // }

    if (buffer.isEmpty) {
      if (ctx.isFinishing) ctx.finish()
      else ctx.pull()
    } else {
      val (emit, nextBuffer) = buffer.splitAt(chunkSize)
      buffer = nextBuffer
      if (ctx.isFinishing)
        ctx.push(addHeader(emit, 1))
      else
        ctx.push(addHeader(emit, 0))
    }

  }

  def addHeader(bytes: ByteString, eof: Short) = {
    val invalidCount = Frame.payloadSize - bytes.length
    val header = Header(streamNum, eof, invalidCount)
    val builder = ByteString.newBuilder ++= header.encode()
    if (invalidCount > 0)
      builder.append(bytes).putBytes(new Array[Byte](invalidCount)).result()
    else
      builder.append(bytes).result()
  }

}
