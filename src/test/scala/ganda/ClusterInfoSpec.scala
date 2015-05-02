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
    //GenerateConfluencePage.generateConfluencePage(tables, KEYSPACE);
    //cluster.keyspaces.filter(_.keyspace_name =="key2").foreach(_.findPossibleLinks)
   // ClusterInfo.compare("key1", "key2")

   // assert (true)
  }

  it  ("Pretty Print CLusterInfo to Confluence") {
     val cluster = ClusterInfo.createClusterInfo(session)
    //val confluenceSoapService: ConfluenceSoapService = new ConfluenceSoapService
    //ConfluenceSoapService.getSpaces("")
    val confluenceUser: String = ""
    val confluencePassword: String = ""
    val token: Token = Token.getInstance
    token.initialise(confluenceUser, confluencePassword)


    var page: Page = new Page
    //val rPage: RemotePage = page.read("RTPE", "Table Template")

    var rPage: RemotePage = page.read("~LH25FM", "GEN")
    println (rPage.getContent.toString)

    val content: String = GenerateConfluencePage.generateConfluencePage(cluster,"key2")
    println (content)
    rPage.setContent(content)
    page.store(rPage)
  }

}