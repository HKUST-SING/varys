package varys.framework.slave

import java.net.InetAddress

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import varys.framework._
import varys.framework.master.Master
import varys.util._
import varys.{Logging, VarysException}

import scala.collection.concurrent.TrieMap
import scala.collection.mutable
import scala.concurrent.{TimeoutException, Future, Await}
import scala.util.{Success, Failure}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

private[varys] object Slave {

  val host = InetAddress.getLocalHost.getHostName
  val port = Option(System.getenv("VARYS_SLAVE_PORT")).getOrElse("1607").toInt
  val masterIp = Option(System.getenv("VARYS_MASTER_IP")).getOrElse("localhost")
  val masterUrl = "varys://" + masterIp + ":" + port;

  private val systemName = "varysSlave"
  private val actorName = "Slave"

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
        throw new VarysException("Invalid master URL: " + varysUrl)
    }
  }

  private[varys] class SlaveActor extends Actor with Logging {

    val flowToActor = TrieMap[Flow, ActorRef]()

    var flowQueue = mutable.Queue[Flow]()

    override def preStart() = {
      context.actorSelection(Master.toAkkaUrl(masterUrl)).resolveOne(10.seconds).onComplete {
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
        flowToActor += flow -> sender

      case FlowCompleted(flow) =>
        flowToActor -= flow

        implicit val timeout = Timeout(1.millis)
        if (flowQueue.nonEmpty) {
          val f = flowQueue.dequeue()
          val actor = flowToActor(f)
          try {
            val reply = (actor ? Start).mapTo[Unit]
            Await.result(reply, timeout.duration)
          } catch {
            case e: TimeoutException =>
              self.tell(FlowCompleted(f), actor)
          }
        }

      case StartSome(flows) =>

        for (actor <- flowToActor.values) {
          actor ! Pause
        }

        flowQueue = mutable.Queue[Flow]() ++ flows

        implicit val timeout = Timeout(1.millis)
        if (flowQueue.nonEmpty) {
          val f = flowQueue.dequeue()
          val actor = flowToActor(f)
          try {
            val reply = (actor ? Start).mapTo[Unit]
            Await.result(reply, timeout.duration)
          } catch {
            case e: TimeoutException =>
              self.tell(FlowCompleted(f), actor)
          }
        }

      case GetLocalCoflows =>

        implicit val timeout = Timeout(1.millis)
        val results = flowToActor.map({
          case (flow, actor) => Future {
            try {
              val reply = (actor ? GetFlowSize).mapTo[FlowSize]
              Some(Await.result(reply, timeout.duration))
            } catch {
              case e: TimeoutException =>
                self.tell(FlowCompleted(flow), actor)
                None
            }
          }
        }).flatMap(Await.result(_, timeout.duration))

        val coflows = results.groupBy(_.coflowId).map({
          case (coflowId, flowSizes) =>
            (coflowId, flowSizes.map(fs => (fs.flow, fs.bytesWritten)).toMap)
        })

        logDebug("Sending LocalCoflows with " + coflows.size + " coflows")
        sender ! LocalCoflows(coflows)
    }
  }

}
