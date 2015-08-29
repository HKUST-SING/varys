package log2coflow.lineparser.common

/**
 * Created by wakira on 15-7-15.
 */
abstract class ParsedLogLine
trait LogLineMatcher {
  def matches(s : String) : Option[ParsedLogLine]
}