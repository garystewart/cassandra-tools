package ganda

import akka.actor.ActorSystem
import akka.testkit.{ImplicitSender, DefaultTimeout, TestKit}
import nl.ing.confluence.rpc.soap.actions.Token
import org.scalatest._

class ClusterInfoSpec  extends TestKit(ActorSystem("ClusterInfoSpec"))
//with DefaultTimeout with ImplicitSender
with FunSpecLike //with Matchers with BeforeAndAfterAll
with TestCassandraCluster {

  it  ("Pretty Print ClusterInfo") {
    val cluster = ClusterInfo.createClusterInfo(session, "LLDS_1")
    //TODO fix me
  //  PrettyPrint.prettyPrintKeyspace(cluster,"key2")
   // assert (true)
  }

  it  ("Pretty Print ClusterInfo to Confluence") {
    GenerateCassandraConfluencePages.generateAllConfluencePages ("","", session,"","", "")
    

  }



  it  ("Confluence Test Read Template") {
    import nl.ing.confluence.rpc.soap.actions.Page
    import nl.ing.confluence.rpc.soap.beans.RemotePage
    val token: Token = Token.getInstance
    token.initialise("", "")

    val page: Page = new Page
    //Find the main Clusters page
    val parentPage: RemotePage = page.read("~npa_minions", "Template")
    println (parentPage.getContent)
  }


  it  ("Practice ClusterInfo ") {

    val allClusters = ClusterInfo.createClusterInfo(session,"LLDS_1")
    val listKeyspace = allClusters.clusterInfoList.flatMap(_.keyspaces).map(_.keyspace_name).toSet
    val listClusterName = allClusters.clusterInfoList.map(_.cluster_name).toSet
    println (listKeyspace)
    println (listClusterName)
  }


  it  ("Check CLusterInfo ") {

    val allClusters = ClusterInfo.createClusterInfo(session, "LLDS_1")
    allClusters.clusterInfoList.foreach(a => println(a.dataCenter))
    allClusters.clusterInfoList.foreach(a => a.keyspaces.foreach(b => println(b.dataCenter)))
  }


//  it ("test graphite api") {
//    var prefix: String = ""
//    for (x <- 1 to 20) {
//      val url = s"http://graphite.europe.intranet/metrics/expand?query=${prefix}LLDS.Cassandra.*"
//      val result = scala.io.Source.fromURL(url).mkString
//      //println(url + " " + result)
//      val js = Json.parse(result)
//      println(url.toString + " " + (js \\ "text").toList)
//      println(js)
//      prefix = prefix + "*."
//    }
//  }

    it ("test ops center api") {
      val host  = "localhost:8888"
      val uname = "admin"
      val pword = "admin"

      val opscenter = OpsCenter.createOpsCenterClusterInfo(host, uname, pword, "LLDS_1_DEV" )
   }



  it ("test json parse") {
    testSpec.testS
  }

}



