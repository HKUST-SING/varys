package varys.framework

import java.util.Date

case class Flow(sIP: String,
                sPort: Int,
                dIP: String,
                dPort: Int) extends Serializable {

  val startTime = new Date(System.currentTimeMillis)
}
