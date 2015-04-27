package ganda

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, DefaultTimeout, TestKit}
import com.typesafe.config.ConfigFactory
import org.scalatest._


class ClusterInfoSpec  extends TestKit(ActorSystem("ClusterInfoSpec"))
//with DefaultTimeout with ImplicitSender
with FunSpecLike //with Matchers with BeforeAndAfterAll
with TestCassandraCluster {

  it  ("Pretty Print CLusterInfo") {
    val cluster = ClusterInfo.createClusterInfo(session)
    PrettyPrint.prettyPrintKeyspace(cluster,"key1")
    PrettyPrint.prettyPrintKeyspace(cluster,"key2")
    //cluster.keyspaces.filter(_.keyspace_name =="key2").foreach(_.findPossibleLinks)
   // ClusterInfo.compare("key1", "key2")

   // assert (true)
  }

}