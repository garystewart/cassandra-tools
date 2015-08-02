package team.supernova.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import com.datastax.driver.core.{Cluster, ProtocolOptions, Session, SimpleStatement}
import team.supernova.actor.Controller.GetClusterGroup
import team.supernova.{AllClusters, ClusterInfo, OpsCenter}


class Controller extends Actor with ActorLogging  {
  import ClusterInfoActor.{ClusterInfoDone, GetClusterInfo}
  import Controller.Done

  //var clusterInfo: ActorRef = ActorRef.noSender
  var counter = 0
  var clusterInfoList: List[ClusterInfo] = List.empty


  override def receive: Receive = {
    case GetClusterGroup(requester, clusterGroup, session) => {
      log.info(s"GetClusterGroup - $clusterGroup")

      import scala.collection.JavaConversions._
      //import scala.collection.SortedSet

      val clusterRes = session.execute(new SimpleStatement(s"select * from cluster where group='$clusterGroup'"))
      val clusterList= clusterRes.foldLeft() { (a, row) =>
        val cluster_name = row.getString("cluster_name")
        //println(s"$cluster_name - starting")

        //get OpsCenter details
        val ops_uname = row.getString("ops_uname")
        val ops_pword = row.getString("ops_pword")
        val ops_hosts = row.getString("opscenter")

        //cluster config
        val uname = row.getString("uname")
        val pword = row.getString("pword")
        val hosts: List[String] = row.getString("hosts").split(",").toList

        //graphite
        val graphite_host = row.getString("graphite")
        val graphana_host = row.getString("graphana")

        val clusterInfo = context.actorOf(Props[ClusterInfoActor])
        counter += 1

        clusterInfo !  GetClusterInfo ( requester, clusterGroup, cluster_name,
          hosts, uname, pword,
          graphite_host, graphana_host,
          ops_hosts,ops_uname, ops_pword )

      }

    }
    case ClusterInfoDone(requester, clusterInfo) => {
      log.info(s"GetClusterInfo - Done message received - counter= $counter")
      counter -= 1
      clusterInfoList = clusterInfoList ++ List(clusterInfo)
      if (counter == 0 ) {
        //TODO - update confluence!!!
        log.info(s"Ready to update confluence - Total:  ${clusterInfoList.size}")
        val allClusters = AllClusters(clusterInfoList)
        requester ! Done (allClusters)
        context.stop(self)
      }
    }
  }
}

object Controller {
  case class GetClusterGroup (requester: ActorRef, clusterGroup: String, session: Session)
  case class  Done (allClusters: AllClusters)
}








//


class ClusterInfoActor extends Actor with ActorLogging  {
  import ClusterInfoActor.{ClusterInfoDone, GetClusterInfo}

  override def receive: Receive = {
    case GetClusterInfo(requester,clusterGroup, clusterName, hosts, uname, pword, graphite_host, graphana_host,ops_hosts, ops_uname, ops_pword ) => {
      log.info(s"GetClusterInfo - message received")
      log.info(s"GetClusterInfo - processing $clusterName")

      //TODO get OpsCenter Info via Actor!
      val opsCenterClusterInfo = OpsCenter.createOpsCenterClusterInfo(ops_hosts, ops_uname, ops_pword, clusterName )

      lazy val clusSes: Session =
        Cluster.builder().
          addContactPoints(hosts: _*).
          withCompression(ProtocolOptions.Compression.SNAPPY).
          withCredentials(uname, pword).
          //withPort(port).
          build().
          connect()

      //TODO add ops Center Info!!!!! via messages!!!!!
      val clusterInfo = ClusterInfo(clusSes.getCluster.getMetadata,  opsCenterClusterInfo, graphite_host, graphana_host)
      clusSes.close()

      sender ! ClusterInfoDone(requester, clusterInfo)

      //TODO check for better error handling
      context.stop(self)
    }
  }

}


object ClusterInfoActor {
  case class GetClusterInfo (requester: ActorRef, clusterGroup: String, clusterName: String,
                             hosts: List[String], uname: String, pword: String,
                             graphite_host: String,graphana_host: String,
                             ops_hosts: String, ops_uname: String, ops_pword: String)
  case class ClusterInfoDone (requester: ActorRef, clusterInfo: ClusterInfo)
}









