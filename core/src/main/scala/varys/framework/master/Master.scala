package varys.framework.master

import java.util.Date
import java.net.InetAddress

import akka.actor._

import scala.collection.mutable
import scala.collection.concurrent.TrieMap
import scala.collection.mutable.ArrayBuffer
import varys.framework._
import varys.{Utils, Logging, VarysException}
import varys.util.AkkaUtils

private[varys] object Master {

  val REMOTE_SYNC_PERIOD_MILLIS = System.getProperty("varys.framework.remoteSyncPeriod", "100").toInt

  val host = Option(System.getenv("VARYS_MASTER_IP")).getOrElse(InetAddress.getLocalHost.getHostAddress)
  val port = Option(System.getenv("VARYS_MASTER_PORT")).getOrElse("1606").toInt

  val systemName = "varysMaster"
  val actorName = "Master"

  val ipToSlave = TrieMap[String, ActorRef]()
  val slaveToCoflows = TrieMap[ActorRef, LocalCoflows]()

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

  def clustering(flows: Array[Flow]): Map[String, Array[Flow]] = {

    val epsilon = 100L

    val sortedIndices = flows.indices.sortBy(flows(_).startTime)
    var lastStartTime = 0L
    var currentCluster = -1

    val clusterIds = sortedIndices.map(i => {
      if (lastStartTime + epsilon < flows(i).startTime) {
        currentCluster += 1
      }
      lastStartTime = flows(i).startTime
      currentCluster
    })

    clusterIds.zip(sortedIndices).groupBy(_._1).map {
      case (id, tuples) => (id.toString, tuples.map(tuple => flows(tuple._2)).toArray)
    }

  }

  def getSchedule(slaves: Array[String], coflows: Map[String, Map[Flow, Long]]): Map[String, Array[Flow]] = {

    val sortedCoflow = coflows.map({
      case (coflowId, flowSizes) => (coflowId, flowSizes.values.sum)
    }).toArray.sortBy(_._2).map(_._1)

    val slaveFlows = slaves.map(_ -> ArrayBuffer[Flow]()).toMap

    for (coflowId <- sortedCoflow) {
      for (flow <- coflows(coflowId).toArray.sortBy(_._2).map(_._1)) {
        slaveFlows(flow.sIP) += flow
      }
    }
    slaveFlows.map({ case (slave, flows) => (slave, flows.toArray) })
  }

  private[varys] class MasterActor extends Actor with Logging {

    override def preStart() {

      logInfo("Starting Varys master at varys://" + host + ":" + port)

      Utils.scheduleDaemonAtFixedRate(0, REMOTE_SYNC_PERIOD_MILLIS) {
        self ! MergeAndSync
      }
    }

    override def receive = {

      case LocalCoflows(ip, coflows) =>
        ipToSlave(ip) = sender
        slaveToCoflows(sender) = LocalCoflows(ip, coflows)
        context.watch(sender)

      case Terminated(actor) =>
        slaveToCoflows.get(actor).foreach {
          ipToSlave -= _.host
        }
        slaveToCoflows -= actor
        context.unwatch(actor)

      case MergeAndSync =>

        val phase1 = System.currentTimeMillis

        val coflows = slaveToCoflows.values.flatMap(_.coflows).groupBy(_._1).map({
          case (k, vs) => (k, vs.map(_._2).fold(mutable.Map[Flow, Long]())(_ ++ _).toMap)
        })
        val flowSize = coflows.values.fold(mutable.Map[Flow, Long]())(_ ++ _).toMap

        val cluster = clustering(flowSize.keys.toArray)
        val flowClusters = cluster.map({
          case (k, fs) => (k, fs.map(f => (f, flowSize(f))).toMap)
        })

        val phase2 = System.currentTimeMillis

        val scores = coflows.map({ case (coflowId, flowSizes) =>
          val trueCluster = flowSizes.keys.toSet
          var precision = 0.0
          var recall = 0.0
          cluster.values.foreach(flows => {
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
        logDebug(scores.mkString(" | "))

        val phase3 = System.currentTimeMillis

        getSchedule(ipToSlave.keys.toArray, flowClusters).foreach({
          case (ip, flows) => ipToSlave.get(ip).foreach(_ ! StartSome(flows))
        })

        val phase4 = System.currentTimeMillis

        logDebug("[Scheduler] " + flowSize.size + " flows "
          + "clustering: " + (phase2 - phase1) + " ms, "
          + "score: " + (phase3 - phase2) + " ms, "
          + "schedule: " + (phase4 - phase3) + " ms")

    }
  }

}
