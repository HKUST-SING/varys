package log2coflow.lineparser.common

/**
 * Created by wakira on 15-7-15.
 */
object ContainerInfoLine extends LogLineMatcher{
  final val regex = """Container: (\S+) on (\S+)""".r
  override def matches(s : String) = s match {
    case regex(container, host) => Some(new ContainerInfoLine(container, host))
    case _ => None
  }

  // a quick unit test
  def main (args: Array[String]) {
    val s = ContainerInfoLine.matches("Container: container_1436860055866_0006_01_000001 on controller_46119")
    s match {
      case Some(ContainerInfoLine(c, h)) => println(c + " " + h)
      case None => println("NG")
    }
    val s2 = ContainerInfoLine.matches("Pontiner: container_1436860055866_0006_01_000001 on controller_46119")
    s2 match {
      case Some(ContainerInfoLine(c, h)) => println(c + " " + h)
      case None => println("NG")
    }
  }
}

case class ContainerInfoLine(container: String, host: String) extends ParsedLogLine
