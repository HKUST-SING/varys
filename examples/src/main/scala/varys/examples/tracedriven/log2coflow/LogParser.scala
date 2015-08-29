/**
 * Created by wakira on 15-7-15.
 */
package log2coflow

import log2coflow.lineparser.common.{ParsedLogLine, LogLineMatcher}

abstract class LogParser(input : Iterator[String]) {
  def run() = {
    // FIXME might require preprocessing to seperate different coflows in one log (see ApplciationDescription)
    input.foreach(processLine)
    buildCoflow
  }

  // helper function that try to match pattern using matchers for line l
  def tryMatchers(matchers: List[LogLineMatcher], l : String): Option[ParsedLogLine] =
    if (matchers.isEmpty) None
    else
      matchers.head.matches(l) match {
        case Some(p) => Some(p)
        case None => tryMatchers(matchers.tail, l)
      }

  def processLine(l : String) : Unit
  def buildCoflow : CoflowDescription
}
