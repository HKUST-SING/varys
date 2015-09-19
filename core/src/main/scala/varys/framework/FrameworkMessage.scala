package varys.framework

import akka.actor.ActorRef

private[varys] sealed trait FrameworkMessage extends Serializable

// Internal Messages
private[varys] object MergeAndSync

private[varys] case class GetClientFlows(slave: ActorRef)

// Slave <-> Master
private[varys] case class LocalCoflows(host: String,
                                       coflows: Map[String, Map[Flow, Long]])
  extends FrameworkMessage

private[varys] case class StartSome(flows: Array[Flow])
  extends FrameworkMessage

// Client to Slave
private[varys] case class ClientCoflows(coflows: Map[String, Map[Flow, Long]])
  extends FrameworkMessage

private[varys] case class Pause(flow: Flow)

private[varys] case class Start(flow: Flow)
