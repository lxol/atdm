package sample.stream
import org.scalatest._

class HelloSpec extends FlatSpec with Matchers {

  "Header decoder" should "should return the encoded Header" in {
    val header = Header(2, 1, 111)
    Header(header.encode()) should be(Header(2, 1, 111)) //should == (true)

  }

  "Header encoded length" should "be 512 bytes" in {
    val header = Header(2, 1, 112)
    header.encode().length should be(512)
  }

}
