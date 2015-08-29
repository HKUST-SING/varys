package varys.examples.tracedriven
import java.io._
import java.net._

import varys.util.AkkaUtils
import varys.{Logging, Utils}
import varys.framework.client._
import varys.framework._

import scala.concurrent.duration._
import scala.concurrent.{Future, Await, ExecutionContext}

/**
 * Created by wakira on 15-7-17.
 */


case class WorkerOnline()
case class PutComplete()
case class GetComplete()

object Worker extends Logging {
  class TestListener extends ClientListener with Logging {
    def connected(id: String) {
      logInfo("Connected to master, got client ID " + id)
    }

    def disconnected() {
      logInfo("Disconnected from master")
      System.exit(0)
    }

    override def coflowRejected(coflowId: String, rejectMessage: String): Unit = {
      logInfo(coflowId + " was rejected: " + rejectMessage)
      System.exit(0)
    }
  }

  private val traceMasterUrlRegex = "([^:]+):([0-9]+)".r

  // ExecutionContext for Futures
  implicit val futureExecContext = ExecutionContext.fromExecutor(Utils.newDaemonCachedThreadPool())

  var sock: Socket = null
  var oos: ObjectOutputStream = null
  var ois: ObjectInputStream = null
  var jobMission : JobMission = null

  private def createSocket(host: String, port: Int): Socket = {
    var retriesLeft = TraceUtils.WORKER_NUM_RETRIES
    while (retriesLeft > 0) {
      try {
        val sock = new Socket(host, port)
        return sock
      } catch {
        case e => {
          logWarning("Failed to connect to " + host + ":" + port + " due to " + e.toString)
        }
      }
      Thread.sleep(TraceUtils.WORKER_RETRY_INTERVAL_MS)
      retriesLeft -= 1
    }
    null
  }

  def main(args: Array[String]) {
    if (args.length < 2) {
      println("USAGE: TraceWorker <varysMasterUrl> <traceMasterUrl>")
      System.exit(1)
    }

    val url = args(0)
    val tUrl = args(1)

    var masterHost: String = null
    var masterPort: Int = 0

    tUrl match {
      case traceMasterUrlRegex(h, p) =>
        masterHost = h
        masterPort = p.toInt
      case _ =>
        logError("Invalid traceMasterUrl: " + tUrl)
        logInfo("traceMasterUrl should be given as host:port")
        System.exit(1)
    }

    // Connect to trace master, retry silently if required
    sock = createSocket(masterHost, masterPort)
    if (sock == null) {
      System.exit(1)
    }

    oos = new ObjectOutputStream(sock.getOutputStream)
    oos.flush()
    ois = new ObjectInputStream(sock.getInputStream)

    // Mark start
    oos.writeObject(WorkerOnline())
    oos.flush()

    // Receive JobMission
    jobMission = ois.readObject.asInstanceOf[JobMission]
    logInfo("Received JobMission")

    val listener = new TestListener
    val client = new VarysClient("TraceWorker", url, listener)
    client.start()

    logInfo("Varys start Putting")
    /*
    val putFutureList = Future.traverse(jobMission.putList)(x => Future{
      client.putFake(x.id, jobMission.coflowId, x.size, 1)
      logInfo("Varys put id " + x.id + " with size " + x.size.toString)
    })
    Await.result(putFutureList, Duration.Inf)
    */
    jobMission.putList.foreach(x => {
      val tempFile = new File("tracedriventmp", "coflow-"+jobMission.coflowId+"-flow-"+x.id.toString+".tmp")
      val putData = Array.tabulate[Byte](x.size)(_.hashCode().toByte)
      val outputStream = new FileOutputStream(tempFile)
      try {
        outputStream.write(putData)
      } finally {
        outputStream.close()
      }
      client.putFile(x.id, tempFile.getAbsolutePath, jobMission.coflowId, x.size, 1)
      //client.putObject[Array[Byte]](x.id, putData, jobMission.coflowId, x.size, 1)
      logInfo("Varys put id " + x.id + " with size " + x.size.toString)
    })
    logInfo("Varys Put Completed")

    oos.writeObject(PutComplete())
    oos.flush()

    ois.readObject().asInstanceOf[StartGetting]
    logInfo("Received StartGetting")
    //Thread.sleep(1000) // FIXME for debug

    if (jobMission.getList.nonEmpty) {
      logInfo("Varys start Getting")
      val getFutureList = Future.traverse(jobMission.getList)(x => Future {
        client.getFile(x.id, jobMission.coflowId)
        //client.getObject[Array[Int]](x.id, jobMission.coflowId)
        logInfo("asking Varys to get id " + x.id)
      })
      Await.result(getFutureList, Duration.Inf)
      logInfo("Get Complete")
    }
    oos.writeObject(GetComplete())
    oos.flush()

    ois.readObject().asInstanceOf[StopWorker]
    logInfo("Worker finished")

    if (sock != null)
      sock.close()
    System.exit(0)
  }
}
