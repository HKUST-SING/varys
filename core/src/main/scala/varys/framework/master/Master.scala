package varys.framework.master

import java.net.InetAddress

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout

import scala.collection.mutable
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{TimeoutException, Await, Future}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import varys.framework._
import varys.{Utils, Logging, VarysException}
import varys.util.AkkaUtils

private[varys] object Master {

  val systemName = "varysMaster"
  val actorName = "Master"

  val host = Option(System.getenv("VARYS_MASTER_IP")).getOrElse(InetAddress.getLocalHost.getHostName)
  val port = Option(System.getenv("VARYS_MASTER_PORT")).getOrElse("1606").toInt

  def main(args: Array[String]) {
    val (actorSystem, _) = AkkaUtils.createActorSystem(systemName, host, port)
    actorSystem.actorOf(Props(new MasterActor), name = actorName)

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

  def dbscan(flows: Array[Flow]): Map[String, Array[Flow]] = {

    val Epsilon = 3
    val MIN_DBSCAN_POINTS = 5
    val unvisited = mutable.Set() ++ flows
    val flowToCluster = mutable.Map[Flow, String]()
    var currentCluster = 0
    while (unvisited.nonEmpty) {
      val flow = unvisited.head
      unvisited -= flow
      val neighbors = mutable.Set() ++ flows.filter(flowDistance(_, flow) < Epsilon)
      if (neighbors.size < MIN_DBSCAN_POINTS) {
        flowToCluster(flow) = (-1).toString
      } else {
        flowToCluster(flow) = currentCluster.toString
        while (neighbors.nonEmpty) {
          val f = neighbors.head
          neighbors -= f
          unvisited -= f
          val newNeighbors = flows.filter(flowDistance(f, _) < Epsilon).toSet
          if (newNeighbors.size >= MIN_DBSCAN_POINTS) {
            neighbors ++= newNeighbors
          }
          if (!flowToCluster.contains(f)) {
            flowToCluster(f) = currentCluster.toString
          }
        }
        currentCluster += 1
      }
    }

    flowToCluster.groupBy(_._2).map({
      case (coflowId, map) => (coflowId, map.keys.toArray)
    })
  }

  def flowDistance(flow: Flow, other: Flow): Long = {
    Math.abs(flow.startTime.getTime - other.startTime.getTime) / 2000 + {
      if (flow.dPort == other.dPort) 0 else 1
    } + {
      if (flow.sPort == other.sPort) 0 else 1
    } + {
      if (flow.sIP == other.sIP) 0 else 1
    } + {
      if (flow.dIP == other.dIP) 0 else 1
    }
  }

  def getSchedule(slaves: Array[String], coflows: Map[String, Map[Flow, Long]]): Map[String, Array[Flow]] = {

    val sortedCoflow = coflows.map({
      case (coflowId, flowSizes) => (coflowId, flowSizes.values.sum)
    }).toArray.sortBy(_._2).map(_._1)

    val slaveFlows = slaves.map(_ -> ArrayBuffer[Flow]()).toMap

    for (coflowId <- sortedCoflow) {
      for (flow <- coflows(coflowId).keys) {
        slaveFlows(flow.sIP) += flow
      }
    }
    slaveFlows.map({ case (slave, flows) => (slave, flows.toArray) })
  }

  private[varys] class MasterActor extends Actor with Logging {

    val REMOTE_SYNC_PERIOD_MILLIS = System.getProperty("varys.framework.remoteSyncPeriod", "80").toInt

    val ipToSlave = TrieMap[String, ActorRef]()

    var coflows = Map[String, Map[Flow, Long]]()
    var flowClusters = Map[String, Map[Flow, Long]]()

    override def preStart() {

      logInfo("Starting Varys master at varys://" + host + ":" + port)

      Utils.scheduleDaemonAtFixedRate(0, REMOTE_SYNC_PERIOD_MILLIS) {

        val start = System.currentTimeMillis

        implicit val timeout = Timeout(20.millis)
        val results = ipToSlave.map({
          case (ip, actor) => Future {
            try {
              val reply = (actor ? GetLocalCoflows).mapTo[LocalCoflows]
              Some(Await.result(reply, timeout.duration))
            } catch {
              case e: TimeoutException =>
                self.tell(SlaveDisconnected(ip), actor)
                None
            }
          }
        }).flatMap(Await.result(_, timeout.duration))

        val phase1 = System.currentTimeMillis

        coflows = results.flatMap(_.coflows).groupBy(_._1).map({
          case (k, vs) => (k, vs.map(_._2).reduce(_ ++ _))
        })
        val flowSize = coflows.values.reduce(_ ++ _)

        val cluster = dbscan(flowSize.keys.toArray)
        flowClusters = cluster.map({
          case (k, fs) => (k, fs.map(f => (f, flowSize(f))).toMap)
        })

        val phase2 = System.currentTimeMillis

        val scores = coflows.map({ case (coflowId, flowSizes) =>
          val trueCluster = flowSizes.keys.toSet
          var precision = 0.0
          var recall = 0.0
          flowClusters.values.map(_.keys).foreach(flows => {
            val predictedCluster = flows.toSet
            val intersection = trueCluster & predictedCluster
            val p = intersection.size.toDouble / predictedCluster.size
            val r = intersection.size.toDouble / trueCluster.size
            if (precision < p) {
              precision = p
            }
            if (recall < r) {
              recall = r
            }
          })
          (coflowId, Map("precision" -> precision, "recall" -> recall))
        })
        logInfo("Identification scores: " + scores)

        val phase3 = System.currentTimeMillis

        getSchedule(ipToSlave.keys.toArray, coflows).foreach({
          case (ip, flows) => ipToSlave(ip) ! StartSome(flows)
        })

        val phase4 = System.currentTimeMillis

        logInfo("[Scheduler] "
          + "sync: " + (phase1 - start) + " ms, "
          + "cluster: " + (phase2 - phase1) + " ms, "
          + "score: " + (phase3 - phase2) + " ms, "
          + "schedule: " + (phase4 - phase3))
      }
    }

    override def receive = {

      case RegisterSlave(ip) =>
        ipToSlave += ip -> sender
        logInfo("Slave connected from " + ip)

      case SlaveDisconnected(ip) =>
        ipToSlave -= ip
        logInfo("Slave disconnected from " + ip)
    }
  }

}
