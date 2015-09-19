package varys.framework.client

import java.io._
import java.net._
import java.util.concurrent.atomic._

import akka.actor._
import akka.util.Timeout
import varys.framework._
import varys.framework.slave.Slave
import varys.util._
import varys.{Utils, Logging}

import scala.collection.concurrent.TrieMap
import scala.concurrent.{TimeoutException, Await}
import scala.concurrent.duration._

/**
 * The VarysOutputStream enables Varys on OutputStream
 * It is implemented as a wrapper on top of another OutputStream instance.
 * Currently, works only directly on sockets.
 */
class VarysOutputStream(val sock: Socket,
                        val coflowId: String)
  extends OutputStream() with Logging {

  val rawStream = sock.getOutputStream
  val canProceed = new AtomicBoolean(true)
  val canProceedLock = new Object
  val bytesWritten = new AtomicLong(0L)
  val flow = new Flow(sock.getLocalAddress.getHostAddress, sock.getLocalPort,
    sock.getInetAddress.getHostAddress, sock.getPort)

  VarysOutputStream.register(this)

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

  private def preWrite() {
    while (!canProceed.get) {
      canProceedLock.synchronized {
        canProceedLock.wait()
      }
    }
  }

  private def postWrite(writeLen: Long) {
    bytesWritten.addAndGet(writeLen)
  }

  override def write(b: Array[Byte], off: Int, len: Int) = synchronized {
    preWrite()
    rawStream.write(b, off, len)
    postWrite(len)
  }

  override def close() {
    VarysOutputStream.close(this)
    rawStream.close()
  }
}

private[client] object VarysOutputStream extends Logging {

  val REMOTE_SYNC_PERIOD_MILLIS = System.getProperty("varys.framework.remoteSyncPeriod", "100").toInt

  val host = InetAddress.getLocalHost.getHostAddress
  val slavePort = Option(System.getenv("VARYS_SLAVE_PORT")).getOrElse("1607").toInt
  val slaveUrl = "varys://" + host + ":" + slavePort
  val clientName = host.replace(".", "-") + "-" + System.currentTimeMillis
  val (actorSystem, _) = AkkaUtils.createActorSystem(clientName, host, 0)
  val clientActor = actorSystem.actorOf(Props(new VarysOutputStreamActor))

  val flowToStream = TrieMap[Flow, VarysOutputStream]()

  def register(vos: VarysOutputStream) {
    flowToStream(vos.flow) = vos
  }

  def close(vos: VarysOutputStream) {
    flowToStream -= vos.flow
  }

  private[client] class VarysOutputStreamActor extends Actor with Logging {

    override def preStart() = {

      val slave = {
        try {
          implicit val timeout = Timeout(1.second)
          val reply = context.actorSelection(Slave.toAkkaUrl(slaveUrl)).resolveOne()
          Some(Await.result(reply, timeout.duration))
        } catch {
          case e: TimeoutException =>
            logInfo("Cannot connect to slave; fallback to nonblocking mode")
            None
          case e: ActorNotFound =>
            logInfo("Cannot connect to slave; fallback to nonblocking mode")
            None
        }
      }
      slave.foreach(actor =>
        Utils.scheduleDaemonAtFixedRate(0, REMOTE_SYNC_PERIOD_MILLIS) {
          self ! GetClientFlows(actor)
        })
    }

    override def receive = {

      case Pause(flow) =>
        flowToStream.get(flow).foreach(_.canProceed.set(false))

      case Start(flow) =>
        flowToStream.get(flow).foreach(vos => {
          vos.canProceed.set(true)
          vos.canProceedLock.synchronized {
            vos.canProceedLock.notifyAll()
          }
        })

      case GetClientFlows(slave) =>
        val coflows = flowToStream.values.groupBy(_.coflowId).map {
          case (coflowId, streams) => (coflowId, streams.map(vos => {
            (vos.flow, vos.bytesWritten.get())
          }).toMap)
        }
        slave ! ClientCoflows(coflows)
    }
  }

}
