package ganda

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, DefaultTimeout, TestKit}
import com.typesafe.config.ConfigFactory
import nl.ing.confluence.plugins.servlet.soap_axis.confluenceservice.ConfluenceSoapService
import nl.ing.confluence.rpc.soap.actions.{Token, Page}
import nl.ing.confluence.rpc.soap.beans.RemotePage
import org.scalatest._


class ClusterInfoSpec  extends TestKit(ActorSystem("ClusterInfoSpec"))
//with DefaultTimeout with ImplicitSender
with FunSpecLike //with Matchers with BeforeAndAfterAll
with TestCassandraCluster {

  it  ("Pretty Print CLusterInfo") {
    val cluster = ClusterInfo.createClusterInfo(session)
    //PrettyPrint.prettyPrintKeyspace(cluster,"key1")
    PrettyPrint.prettyPrintKeyspace(cluster,"key2")
   // assert (true)
  }

  it  ("Pretty Print CLusterInfo to Confluence") {

    GenerateConfluencePage.generateConfluencePages ("", session,"","")
  }

}