package varys.framework.scheduler

import scala.collection.mutable.HashMap

import varys.framework.Flow

private[scheduler] case class Coflow(coflowId: Int) {
  var currentJobQueue: Int = 0
  var sizeSoFar: Long = 0
  val flows = new HashMap[String, Array[Flow]]()

  override def toString: String = "Coflow(" + coflowId + ", " + "currentJobQueue="+ 
    currentJobQueue + ", " + "sizeSoFar=" + sizeSoFar + ")"
}
