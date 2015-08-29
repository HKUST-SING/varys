package varys.examples.tracedriven

import java.io.{ObjectInputStream, ObjectOutputStream}
import java.net.{Socket, ServerSocket}
import java.util.concurrent.atomic.AtomicInteger
import log2coflow.{FlowDescription, YarnMapReduceLogParser, CoflowDescription}
import varys.{Logging, Utils}
import varys.framework.{CoflowType}
import varys.framework.client.{VarysClient, ClientListener}

/**
 * Created by wakira on 15-7-17.
 */


case class PutDescription(id: String, size: Int)
case class GetDescription(id: String)

case class JobMission(coflowId: String, putList: List[PutDescription], getList: List[GetDescription])
case class StartGetting()
case class StopWorker()

object Master extends Logging {

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

  class MasterThread (val coflowId: String, val coflowDescription: CoflowDescription,
                       val listenPort: Int)
    extends Thread("TraceMasterThread") with Logging {

    val nodesInCoflow = coflowDescription.nodes.toArray

    val HEARTBEAT_SEC = System.getProperty("varys.framework.heartbeat", "1").toInt
    var serverSocket: ServerSocket = new ServerSocket(listenPort)

    var assignedWorkers = new AtomicInteger()
    var putCompletedWorkers = new AtomicInteger()
    var getCompletedWorkers = new AtomicInteger()
    var shutdownWorkers = new AtomicInteger()
    var connectedWorkers = new AtomicInteger()
    var stopServer = false
    this.setDaemon(true)

    override def run(): Unit = {
      var threadPool = varys.Utils.newDaemonCachedThreadPool()
      try {
        while (!stopServer && !finished) {
          var clientSocket: Socket = null
          try {
            serverSocket.setSoTimeout(HEARTBEAT_SEC * 1000)
            clientSocket = serverSocket.accept
          } catch {
            case e: Exception => {
              if (stopServer) {
                logInfo("Stopping TraceMaster" )
              }
            }
          }

          if  (clientSocket != null) {
            try {
              threadPool.execute(new Thread {
                override def run(): Unit = {
                  val oos = new ObjectOutputStream(clientSocket.getOutputStream)
                  oos.flush()
                  val ois = new ObjectInputStream(clientSocket.getInputStream)

                  try {
                    // Mark start of worker connection
                    ois.readObject.asInstanceOf[WorkerOnline]
                    connectedWorkers.getAndIncrement()

                    // assign a node for this worker
                    val nodeForWorker: String = getUnassignedNode
                    // filter flows with the node as source to construct putList
                    val putList = coflowDescription.flows.filter(_.source == nodeForWorker).map(flow =>
                      //new PutDescription("flow-" + flow.source + "-" + flow.dest + "-" + flow.uid.toString, flow.size)
                      new PutDescription("flow-" + flow.uid.toString, flow.size)
                    )
                    // filter flows with the node as dest to construct getList
                    val getList = coflowDescription.flows.filter(_.dest == nodeForWorker).map(flow =>
                      //new GetDescription("flow-" + flow.source + "-" + flow.dest + "-" + flow.uid.toString)
                      new GetDescription("flow-" + flow.uid.toString)
                    )

                    // send coflowId and JobMission
                    oos.writeObject(JobMission(coflowId, putList, getList))

                    // wait for ALL workers to complete put
                    ois.readObject().asInstanceOf[PutComplete]
                    putCompletedWorkers.incrementAndGet()
                    while (putCompletedWorkers.get() < nodesInCoflow.length) {
                      Thread.sleep(500) // FIXME choose appropriate value
                    }

                    // send StartGetting
                    logInfo("sending StartGetting")
                    oos.writeObject(StartGetting)

                    // wait for ALL workers to complete get
                    ois.readObject().asInstanceOf[GetComplete]
                    getCompletedWorkers.incrementAndGet()
                    while (getCompletedWorkers.get() < nodesInCoflow.length) {
                      Thread.sleep(50) // FIXME choose appropriate value
                    }

                    logInfo("sending StopWorker")
                    oos.writeObject(StopWorker)
                    oos.flush()
                    shutdownWorkers.incrementAndGet()
                  } catch {
                    case e: Exception => {
                      logWarning ("TraceMaster had a " + e)
                    }
                  } finally {
                    clientSocket.close()
                  }
                }
              })
            } catch {
              // In failure, close socket here; else, client thread will close
              case e: Exception =>
                logError("TraceMaster had a " + e)
                clientSocket.close()
            }
          }

        }
      } finally {
        serverSocket.close()
      }
      // Shutdown the thread pool
      threadPool.shutdown()
    }
    def finished = shutdownWorkers.get() == nodesInCoflow.length

    def getUnassignedNode : String= {
      val index = assignedWorkers.getAndIncrement()
      nodesInCoflow.apply(index)
    }
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      println("USAGE: TraceMaster <varysMasterUrl> <traceLogFile> <listenPort> [<deadlineMillis>] [<sizeMultiplier>]")
      System.exit(1)
    }

    val url = args(0)
    val pathToFile = args(1)
    val listenPort = args(2).toInt
    var deadlineMillis: Long = 0
    var sizeMultiplier = 1.0
    if (args.length >= 4) {
      deadlineMillis = args(3).toLong
    }
    if (args.length == 5) {
      sizeMultiplier = args(4).toDouble
    }

    var fileName: String = null

    // run log2coflow on file
    val input = scala.io.Source.fromFile(pathToFile).getLines()
    var desc = new YarnMapReduceLogParser(input).run()
    // apply multiplier to flow size
    if (sizeMultiplier != 1.0)
      desc = new log2coflow.CoflowDescription(desc.flows.map(f =>
        new FlowDescription(f.source, f.dest, (f.size*sizeMultiplier).toInt, f.uid)))

    val listener = new TestListener
    val client = new VarysClient("TraceMaster", url, listener)
    client.start()
    //client.startDNBD(5678, nInterface)
    //Thread.sleep(5000)

    val varysDesc = new varys.framework.CoflowDescription(
      "Trace-" + fileName,
      CoflowType.SHUFFLE, desc.width, desc.size, deadlineMillis)

    val coflowId = client.registerCoflow(varysDesc)
    logInfo("Registered coflow " + coflowId)

    // Start server after registering the coflow and relevant
    val masterThread = new MasterThread(coflowId, desc, listenPort)
    masterThread.start()
    logInfo("Started MasterThread. Now waiting for it to die.")
    logInfo("Broadcast Master Url: %s:%d".format(
      Utils.localHostName, listenPort))

    // Wait for all slaves to receive
    masterThread.join()
    logInfo("Unregistered coflow " + coflowId)
    client.unregisterCoflow(coflowId)
  }

}
