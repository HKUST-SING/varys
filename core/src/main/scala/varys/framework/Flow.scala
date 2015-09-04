package varys.framework

import java.util.Date

case class Flow(
  val sIP: String,
  val sPort: Int,
  val dIP: String,
  val dPort: Int) extends Serializable {

  val startTime = new Date(System.currentTimeMillis)

  override def toString: String = "Flow(" + sIP + ":" + sPort + " -> " + dIP + ":" + dPort + "@" + startTime + ")"
}
