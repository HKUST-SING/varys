package log2coflow.lineparser.mapreduce
import log2coflow.lineparser.common.{LogLineMatcher, ParsedLogLine}

/**
 * Created by wakira on 15-7-15.
 */
object AttemptAssignmentLine extends LogLineMatcher{
  final val regex = """.*Assigned container (\S+) to (\S+)""".r
  override def matches(s : String) = s match {
    case regex(container, attempt) => Some(new AttemptAssignmentLine(container, attempt))
    case _ => None
  }

  // a quick unit test
  def main (args: Array[String]) {
    val s = AttemptAssignmentLine.matches("2015-07-14 11:43:17,689 INFO [RMCommunicator Allocator] org.apache.hadoop.mapreduce.v2.app.rm.RMContainerAllocator: Assigned container container_1436845481536_0001_01_000002 to attempt_1436845481536_0001_m_000001_0")
    s match {
      case Some(AttemptAssignmentLine(c, a)) => println(c + " " + a)
      case None => println("NG")
    }
    val s2 = AttemptAssignmentLine.matches("Pontiner: container_1436860055866_0006_01_000001 on controller_46119")
    s2 match {
      case Some(AttemptAssignmentLine(c, a)) => println(c + " " + a)
      case None => println("NG")
    }
  }
}

case class AttemptAssignmentLine(container: String, attempt: String) extends ParsedLogLine
