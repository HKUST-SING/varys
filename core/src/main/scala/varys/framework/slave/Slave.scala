package varys.framework.slave

import java.net.InetAddress
import java.util.NoSuchElementException
import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor._
import akka.util.Timeout
import varys.framework._
import varys.framework.master.Master
import varys.util._
import varys.{Utils, Logging, VarysException}

import scala.collection.concurrent.TrieMap
import scala.concurrent.Await
import scala.concurrent.duration._

private[varys] object Slave {

  val REMOTE_SYNC_PERIOD_MILLIS = System.getProperty("varys.framework.remoteSyncPeriod", "100").toInt

  val host = InetAddress.getLocalHost.getHostAddress
  val port = Option(System.getenv("VARYS_SLAVE_PORT")).getOrElse("1607").toInt
  val masterIp = Option(System.getenv("VARYS_MASTER_IP")).getOrElse("127.0.0.1")
  val masterPort = Option(System.getenv("VARYS_MASTER_PORT")).getOrElse("1606")
  val masterUrl = "varys://" + masterIp + ":" + masterPort

  val systemName = "varysSlave"
  val actorName = "Slave"

  val flowToActor = TrieMap[Flow, ActorRef]()
  val flowToSize = TrieMap[Flow, FlowSize]()
  val dstFlowQueue = TrieMap[String, ConcurrentLinkedQueue[Flow]]()

  def main(argStrings: Array[String]) {
    val (actorSystem, _) = AkkaUtils.createActorSystem(systemName, host, port)
    actorSystem.actorOf(Props(new SlaveActor), name = actorName)

    actorSystem.awaitTermination()
  }

  def toAkkaUrl(varysUrl: String): String = {
    val varysUrlRegex = "varys://([^:]+):([0-9]+)".r
    varysUrl match {
      case varysUrlRegex(ip, p) =>
        "akka.tcp://%s@%s:%s/user/%s".format(systemName, ip, p, actorName)
      case _ =>
        throw new VarysException("Invalid slave URL: " + varysUrl)
    }
  }

  private[varys] class SlaveActor extends Actor with Logging {

    val master = {
      implicit val timeout = Timeout(1.second)
      val future = context.actorSelection(Master.toAkkaUrl(masterUrl)).resolveOne()
      Await.result(future, timeout.duration)
    }

    override def preStart() = {
      Utils.scheduleDaemonAtFixedRate(0, REMOTE_SYNC_PERIOD_MILLIS) {
        self ! GetLocalCoflows
      }
    }

    override def receive = {

      case FlowSize(coflowId, flow, bytesWritten) =>

        flowToSize(flow) = FlowSize(coflowId, flow, bytesWritten)
        flowToActor(flow) = sender

      case FlowCompleted(flow) =>

        logDebug("Client disconnected with " + flow)
        flowToSize -= flow
        flowToActor -= flow

        dstFlowQueue.get(flow.dIP).foreach {
          queue => Option(queue.poll()).foreach(startOne)
        }

      case GetLocalCoflows =>

        val coflows = flowToSize.values.groupBy(_.coflowId).map({
          case (coflowId, flowSizes) =>
            (coflowId, flowSizes.map(fs => (fs.flow, fs.bytesWritten)).toMap)
        })

        logTrace("Sending LocalCoflows with " + coflows.size + " coflows")
        master ! LocalCoflows(host, coflows)

      case StartSome(flows) =>

        logTrace("Received StartSome for " + flows.mkString(", "))

        dstFlowQueue.clear()

        for (f <- flows) {
          try {
            val actor = flowToActor(f)
            actor ! Pause
            if (!dstFlowQueue.contains(f.dIP)) {
              dstFlowQueue(f.dIP) = new ConcurrentLinkedQueue[Flow]
            }
            dstFlowQueue(f.dIP).add(f)
          } catch {
            case e: NoSuchElementException =>
          }
        }

        for (flowQueue <- dstFlowQueue.values) {
          Option(flowQueue.poll).foreach(startOne)
        }
    }

    def startOne(f: Flow) {
      flowToActor.get(f).foreach(_ ! Start)
    }
  }

}
