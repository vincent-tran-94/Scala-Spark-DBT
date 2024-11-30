package sda.parser
import scala.io.Source
object FileReaderUsingI0Source {

  def getContent(file: String): String = {
    Source.fromFile(file).getLines().mkString
   }
}
