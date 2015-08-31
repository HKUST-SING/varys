package varys.framework

import java.util.Date

class Flow(
  val sIP: String,
  val sPort: Int,
  val dIP: String,
  val dPort: Int) extends Serializable {

  val startTime = System.currentTimeMillis

  override def toString: String = "Flow(" + sIP + ":" + sPort + " -> " + dIP + ":" + dPort + ", since " + new Date(startTime) + ")"
}
