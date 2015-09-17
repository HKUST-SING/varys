package varys.framework.client

import java.io._
import java.net._
import java.util.concurrent.atomic._

import akka.actor._
import varys.framework._
import varys.framework.slave.Slave
import varys.util._
import varys.Logging

import scala.concurrent.duration._
import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global

/**
 * The VarysOutputStream enables Varys on OutputStream
 * It is implemented as a wrapper on top of another OutputStream instance.
 * Currently, works only directly on sockets.
 */
class VarysOutputStream(val sock: Socket,
                        val coflowId: String)
  extends OutputStream() with Logging {

  val flow = new Flow(sock.getLocalAddress.getHostAddress, sock.getLocalPort,
    sock.getInetAddress.getHostAddress, sock.getPort)

  val clientName = flow.sIP + "@" + System.currentTimeMillis.toString
  val (actorSystem, _) = AkkaUtils.createActorSystem(clientName, flow.sIP, 0)
  val clientActor = actorSystem.actorOf(Props(new VarysOutputStreamActor))

  val rawStream = sock.getOutputStream
  val canProceed = new AtomicBoolean(true)
  val canProceedLock = new Object
  var bytesWritten = 0L

  override def write(b: Int) = synchronized {
    preWrite()
    rawStream.write(b)
    postWrite(1)
  }

  override def write(b: Array[Byte]) = synchronized {
    preWrite()
    rawStream.write(b)
    postWrite(b.length)
  }

  override def write(b: Array[Byte], off: Int, len: Int) = synchronized {
    preWrite()
    rawStream.write(b, off, len)
    postWrite(len)
  }

  private def preWrite() {
    while (!canProceed.get) {
      canProceedLock.synchronized {
        canProceedLock.wait()
      }
    }
  }

  private def postWrite(writeLen: Long) {
    bytesWritten += writeLen
  }

  private[client] class VarysOutputStreamActor extends Actor with Logging {

    val port = Option(System.getenv("VARYS_SLAVE_PORT")).getOrElse("1607").toInt
    val slaveUrl = "varys://" + flow.sIP + ":" + port

    context.actorSelection(Slave.toAkkaUrl(slaveUrl)).resolveOne(1.millis).onComplete {
      case Success(actor) => actor ! RegisterClient(flow)
      case Failure(e) => logDebug("Cannot connect to slave due to " + e + "; fallback to non-blocking mode")
    }

    override def receive = {
      case Pause =>
        canProceed.set(false)

      case Start =>
        canProceed.set(true)
        canProceedLock.notifyAll()
        sender ! ()

      case GetFlowSize =>
        sender ! FlowSize(coflowId, flow, bytesWritten)
    }
  }

}
