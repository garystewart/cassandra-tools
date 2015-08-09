package team.supernova.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.datastax.driver.core._
import team.supernova.actor.Controller.GetClusterGroup
import team.supernova.{GroupClusters, ClusterInfo, OpsCenter}

import scala.collection.SortedSet


/**
 * Created by Gary Stewart on 4-8-2015.
 *
 */

class Controller extends Actor with ActorLogging  {
  import ClusterInfoActor.{ClusterInfoDone, GetClusterInfo}
  import Controller.Done

  var counter = 0
  var clusterInfoList: SortedSet[ClusterInfo] = SortedSet.empty

  def processClusterInfoPerRow (row: Row, requester: ActorRef, clusterGroup: String) = {
    val cluster_name = row.getString("cluster_name")
    val sequence = row.getInt("sequence")

    //get OpsCenter details
    val ops_uname = row.getString("ops_uname")
    val ops_pword = row.getString("ops_pword")
    val ops_hosts = row.getString("opscenter")

    //cluster config
    val uname = row.getString("uname")
    val pword = row.getString("pword")
    val hosts: List[String] = row.getString("hosts").split(",").toList
    val port = row.getInt("port")

    //graphite
    val graphite_host = row.getString("graphite")
    val graphana_host = row.getString("graphana")

    val clusterInfo = context.actorOf(Props[ClusterInfoActor])
    counter += 1

    clusterInfo !  GetClusterInfo ( requester, clusterGroup, cluster_name,
      hosts, port, uname, pword,
      graphite_host, graphana_host,
      ops_hosts,ops_uname, ops_pword,
      sequence )
  }

  override def receive: Receive = {
    case GetClusterGroup(requester, clusterGroup, session) => {
      log.info(s"GetClusterGroup - $clusterGroup")

      import scala.collection.JavaConversions._
      log.info(s"Querying cassandra - " + session.getCluster.getMetadata.getClusterName + " " + session.getLoggedKeyspace )
      val clusterRes = session.execute(new SimpleStatement(s"select * from cluster where group='$clusterGroup'"))
      val clusterList= clusterRes.foldLeft() { (a, row) =>  processClusterInfoPerRow  (row, requester, clusterGroup)     }
    }

    case ClusterInfoDone(requester, clusterInfo) => {
      log.info(s"GetClusterInfo - Done message received - counter= $counter")
      counter -= 1
      clusterInfoList = clusterInfoList ++ SortedSet(clusterInfo)

      if (counter == 0 ) {
        log.info(s"All rows processeed Total:  ${clusterInfoList.size}")
        val allClusters = GroupClusters(clusterInfoList)
        requester ! Done (allClusters)
        context.stop(self)
      }
    }
  }
}

object Controller {
  case class GetClusterGroup (requester: ActorRef, clusterGroup: String, session: Session)
  case class  Done (allClusters: GroupClusters)
}


class ClusterInfoActor extends Actor with ActorLogging  {
  import ClusterInfoActor.{ClusterInfoDone, GetClusterInfo}

  override def receive: Receive = {
    case GetClusterInfo(requester,clusterGroup, clusterName, hosts, port, uname, pword, graphite_host, graphana_host,ops_hosts, ops_uname, ops_pword, sequence ) => {
      log.info(s"GetClusterInfo - message received")
      log.info(s"GetClusterInfo - processing $clusterName")

      //TODO get OpsCenter Info via Actor!
      //val opsCenterClusterInfo = OpsCenter.createOpsCenterClusterInfo(ops_hosts, ops_uname, ops_pword, clusterName )

      lazy val clusSes: Session =
        Cluster.builder().
          addContactPoints(hosts: _*).
          withCompression(ProtocolOptions.Compression.SNAPPY).
          withCredentials(uname, pword).
          withPort(port).
          build().
          connect()

      //TODO add ops Center Info!!!!! via messages!!!!!
     // val clusterInfo = ClusterInfo(clusSes.getCluster.getMetadata,  opsCenterClusterInfo, graphite_host, graphana_host, sequence)
      val clusterInfo = ClusterInfo(clusSes.getCluster.getMetadata, graphite_host, graphana_host, sequence, ops_hosts, ops_uname, ops_pword)
      clusSes.close()

      sender ! ClusterInfoDone(requester, clusterInfo)

      //TODO check for better error handling
      context.stop(self)
    }
  }

}


object ClusterInfoActor {
  case class GetClusterInfo (requester: ActorRef, clusterGroup: String, clusterName: String,
                             hosts: List[String],port: Int, uname: String, pword: String,
                             graphite_host: String,graphana_host: String,
                             ops_hosts: String, ops_uname: String, ops_pword: String, sequence: Int)
  case class ClusterInfoDone (requester: ActorRef, clusterInfo: ClusterInfo)
}
