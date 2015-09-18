package varys.framework

private[varys] sealed trait FrameworkMessage extends Serializable

private[varys] object MergeAndSync

// Slave <-> Master
private[varys] case object GetLocalCoflows

private[varys] case class LocalCoflows(coflows: Map[String, Map[Flow, Long]])
  extends FrameworkMessage

private[varys] case class RegisterSlave(ip: String)
  extends FrameworkMessage

private[varys] case class SlaveDisconnected(ip: String)
  extends FrameworkMessage

private[varys] case class StartSome(flows: Array[Flow])
  extends FrameworkMessage

// Client to Slave
private[varys] case object GetFlowSize

private[varys] case class FlowSize(coflowId: String,
                                   flow: Flow,
                                   bytesWritten: Long)
  extends FrameworkMessage

private[varys] case class RegisterClient(flow: Flow)
  extends FrameworkMessage

private[varys] case class FlowCompleted(flow: Flow)
  extends FrameworkMessage

private[varys] case object Pause

private[varys] case object Start
