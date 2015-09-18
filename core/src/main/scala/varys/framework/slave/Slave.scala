package varys.framework.slave

import java.net.InetAddress
import java.util.NoSuchElementException
import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import varys.framework._
import varys.framework.master.Master
import varys.util._
import varys.{Logging, VarysException}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable.Set
import scala.collection.JavaConversions._
import scala.concurrent.{Await, Future}
import scala.util.{Success, Failure}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

private[varys] object Slave {

  val host = InetAddress.getLocalHost.getHostAddress
  val port = Option(System.getenv("VARYS_SLAVE_PORT")).getOrElse("1607").toInt
  val masterIp = Option(System.getenv("VARYS_MASTER_IP")).getOrElse("127.0.0.1")
  val masterPort = Option(System.getenv("VARYS_MASTER_PORT")).getOrElse("1606")
  val masterUrl = "varys://" + masterIp + ":" + masterPort;

  val systemName = "varysSlave"
  val actorName = "Slave"

  val flowToActor = TrieMap[Flow, ActorRef]()
  val dstFlowQueue = TrieMap[String, ConcurrentLinkedQueue[Flow]]()

  def main(argStrings: Array[String]) {
    val (actorSystem, _) = AkkaUtils.createActorSystem(systemName, host, port)
    val actor = actorSystem.actorOf(Props(new SlaveActor), name = actorName)

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

    override def preStart() = {
      context.actorSelection(Master.toAkkaUrl(masterUrl)).resolveOne(1.second).onComplete {
        case Success(actor) =>
          actor ! RegisterSlave(host)
          logInfo("Starting Varys slave %s:%d".format(host, port))
        case Failure(e) =>
          logError("Cannot connect to master; exiting")
          sys.exit(1)
      }
    }

    override def receive = {

      case RegisterClient(flow) =>

        logDebug("Client connected with " + flow)
        flowToActor += flow -> sender

      case FlowCompleted(flow) =>

        logDebug("Client disconnected with " + flow)
        flowToActor -= flow

        val f = dstFlowQueue.get(flow.dIP) match {
            case Some(queue) => queue.poll
            case None        => null
        }
        if (f != null) {
          try {
            val actor = flowToActor(f)
            actor ! Start
          } catch {
            case e: NoSuchElementException => {}
          }
        }

      case StartSome(flows) =>

        logTrace("Received StartSome for " + flows.mkString(", "))

        dstFlowQueue.clear

        for (f <- flows) {
          try {
            val actor = flowToActor(f)
            actor ! Pause
            if (!dstFlowQueue.contains(f.dIP)) {
              dstFlowQueue(f.dIP) = new ConcurrentLinkedQueue[Flow]
            }
            dstFlowQueue(f.dIP).add(f)
          } catch {
            case e: NoSuchElementException => {}
          }
        }

        for (flowQueue <- dstFlowQueue.values) {
          val f = flowQueue.poll
          try {
            val actor = flowToActor(f)
            actor ! Start
          } catch {
            case e: NoSuchElementException => {}
          }
        }

      case GetLocalCoflows =>

        implicit val timeout = Timeout(10.millis)
        val replies = Future.sequence(flowToActor.map {
          case (flow, actor) => {
            val reply = (actor ? GetFlowSize).mapTo[FlowSize]
            Future {
              Some(Await.result(reply, timeout.duration))
            } recover {
              case e: Throwable => None
            }
          }
        })
        val results = Await.result(replies, 2 * timeout.duration).flatten

        val coflows = results.groupBy(_.coflowId).map({
          case (coflowId, flowSizes) =>
            (coflowId, flowSizes.map(fs => (fs.flow, fs.bytesWritten)).toMap)
        })

        logTrace("Sending LocalCoflows with " + coflows.size + " coflows")
        sender ! LocalCoflows(coflows)
    }
  }

}
