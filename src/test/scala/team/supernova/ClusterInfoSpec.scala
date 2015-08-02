package team.supernova

import akka.actor.Actor.Receive
import akka.actor.{Actor, Props, ActorSystem}
import akka.testkit._
import team.supernova.reporting.Overview

import nl.ing.confluence.rpc.soap.actions.Token
import org.scalatest._
import scala.concurrent.duration._
import scalaj.http.Http

class ClusterInfoSpec  extends TestKit(ActorSystem("ClusterInfoSpec"))
//with DefaultTimeout with ImplicitSender
with FunSpecLike //with Matchers with BeforeAndAfterAll
with TestCassandraCluster {

//  it  ("Pretty Print ClusterInfo") {
//   // val cluster = ClusterInfo.createClusterInfo(session, "LLDS_1")
//    //TODO fix me
//  //  PrettyPrint.prettyPrintKeyspace(cluster,"key2")
//   // assert (true)
//  }

  it  ("Generate Confluence - NPA_MINIONS") {
    GenerateCassandraConfluencePages.generateAllConfluencePages ("~npa_minions","LLDS_1", session, ConfluenceProp.getConfluenceToken , true)

  }

  it  ("Generate Confluence - KaaS") {
    GenerateCassandraConfluencePages.generateAllConfluencePages ("kaas","LLDS_1", session, ConfluenceProp.getConfluenceToken, false)
  }


  it  ("Confluence Test Read Page") {
    import nl.ing.confluence.rpc.soap.actions.Page
    import nl.ing.confluence.rpc.soap.beans.RemotePage
    val token: Token = ConfluenceProp.getConfluenceToken
    //val pageName = "Test - D3.js chart"
    val pageName = "<Keyspace Name>"

    val page: Page = new Page
    val parentPage: RemotePage = page.read("kaas", pageName)
    println (parentPage.getContent)
  }


//  it  ("Jira Test gen List") {
//    import org.json4s._
//    import org.json4s.jackson.JsonMethods._
//    val host = "jira.europe.intranet"
//    //backlog
//    //https://jira.europe.intranet/rest/greenhopper/1.0/xboard/plan/backlog/data.json?rapidViewId=4879&_=1436941822271
//    //val res = Http(s"https://@$host//rest/greenhopper/1.0/xboard/issue/details.json?rapidViewId=4879&issueIdOrKey=MNS-1126&loadSubtasks=true&_=1436941823440")
//    val res = Http(s"https://@$host/rest/greenhopper/1.0/xboard/plan/backlog/data.json?rapidViewId=4879&_=1436941822271")
//      .header("Authorization", "Basic bGgyNWZtOlN0ZXdhcjA0").header("Accept", "text/json").timeout(connTimeoutMs = 1000, readTimeoutMs = 10000).asString.body
//    println (res)
//  }



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



//    it ("test ops center api") {
//      val host  = "localhost:8888"
//      val uname = "admin"
//      val pword = "admin"
//
//      val opscenter = OpsCenter.createOpsCenterClusterInfo(host, uname, pword, "LLDS_1_DEV" )
//
//      OpsCenter.getTableSize(opscenter.head.login, host, uname, pword, "LLDS_1_DEV","party")
//   }



/*
  it("Actor simple test") {
    case object Finished
    val controller = system.actorOf(Props[Controller])
    val group = "LLDS_1"

    val actorRef = TestActorRef(new Actor {
      def receive = {
        case done: Done => {
          println ("DONE!!!")
          GenerateCassandraConfluencePages.generateAllConfluencePages2 ("~npa_minions",group, ConfluenceProp.getConfluenceToken, done.allClusters)
          //reply to testActor to keep test running until finished!!!
          testActor ! Finished
        }
      }
    })

    controller ! GetClusterGroup (actorRef, group, session)

    expectMsg(300 seconds,Finished)

    //TODO shutdown system
  }
*/
//  it ("test json parse") {
//    testSpec.testS
//  }

}



