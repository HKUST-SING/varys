package log2coflow
import log2coflow.lineparser.common.{ParsedLogLine, ContainerInfoLine, LogLineMatcher}
import log2coflow.lineparser.mapreduce.{DataFetchLine, AttemptAssignmentLine}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by wakira on 15-7-15.
 */

class YarnMapReduceLogParser(input : Iterator[String]) extends LogParser(input) {
  private class ContainerFetchFromAttempt(val source: String, val size: Int, val dest: String)

  final val lineMatchers : List[LogLineMatcher] = List(ContainerInfoLine, AttemptAssignmentLine, DataFetchLine)

  private var currentContainer : String = new String
  private var containerToHost : mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
  private var attemptToContainer : mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
  private var containerFetchFromAttempt : mutable.ListBuffer[ContainerFetchFromAttempt] = new ListBuffer
  private var rawFlowCount = 0

  def processLine(l : String) = {
    val parsedLine = tryMatchers(lineMatchers, l)
    parsedLine match {
      case Some(ContainerInfoLine(container, host)) =>
        currentContainer = container
        containerToHost.put(container, host)
      case Some(AttemptAssignmentLine(container, attempt)) => attemptToContainer.put(attempt, container)
      case Some(DataFetchLine(size, source)) =>
        containerFetchFromAttempt.append(new ContainerFetchFromAttempt(source, size, currentContainer))
      case None => // DO NOTHING
      case _ => assert(false)
    }
  }

  override def buildCoflow = {
    def mapToFlow(c: ContainerFetchFromAttempt) = {
      rawFlowCount += 1 // rawFlowCount will count flows that has same host as dest and source, no problem
      new FlowDescription(containerToHost.get(attemptToContainer.get(c.source).get).get,
        containerToHost.get(c.dest).get, c.size, rawFlowCount)
    }

    new CoflowDescription(containerFetchFromAttempt.map(mapToFlow).toList
      .filter(f => f.source != f.dest)) // filter out local data move
  }
}
