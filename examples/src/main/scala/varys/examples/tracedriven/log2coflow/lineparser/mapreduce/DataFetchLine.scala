package log2coflow.lineparser.mapreduce

import log2coflow.lineparser.common.{ParsedLogLine, LogLineMatcher}

/**
 * Created by wakira on 15-7-16.
 */
object DataFetchLine extends LogLineMatcher{
  final val regex = """.*Read (\S+) bytes from map-output for (\S+)""".r
  override def matches(s : String) = s match {
    case regex(sizeStr, source) => Some(new DataFetchLine(sizeStr.toInt, source)) // FIXME catch exception here
    case _ => None
  }

  // a quick unit test
  def main (args: Array[String]) {
    val s = DataFetchLine.matches("2015-07-14 11:47:22,185 INFO [fetcher#4] org.apache.hadoop.mapreduce.task.reduce.InMemoryMapOutput: Read 24 bytes from map-output for attempt_1436845481536_0001_m_000005_0")
    s match {
      case Some(DataFetchLine(size, source)) => println(size.toString + " " + source)
      case None => println("NG")
      case _ => assert(false)
    }
    val s2 = DataFetchLine.matches("Pontiner: container_1436860055866_0006_01_000001 on controller_46119")
    s2 match {
      case Some(DataFetchLine(size, source)) => println(size.toString + " " + source)
      case None => println("NG")
      case _ => assert(false)
    }
  }
}

case class DataFetchLine(size: Int, source: String) extends ParsedLogLine{
}
