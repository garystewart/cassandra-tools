package team.supernova

import akka.actor.{Actor, Props, ActorSystem}
import akka.testkit._
import org.json4s.jackson.JsonMethods._

import org.scalatest._
import team.supernova.actor.Controller
import team.supernova.actor.Controller.{GetClusterGroup, Done}
import team.supernova.confluence.soap.rpc.soap.actions.{Page, Token}
import team.supernova.confluence.soap.rpc.soap.beans.RemotePage
import team.supernova.confluence.{GenerateCassandraConfluencePages, ConfluenceToken}
import team.supernova.domain.Login

import scalaj.http.Http


/**
 * Created by Gary Stewart on 4-8-2015.
 *
 */
class ClusterInfoSpec  extends TestKit(ActorSystem("ClusterInfoSpec"))
//with DefaultTimeout with ImplicitSender
with FunSpecLike //with Matchers with BeforeAndAfterAll
with TestCassandraCluster {

  val TEST_PROPERTIES="test.properties"
  //val TEST_PROPERTIES="local.properties"
  val GROUP="LLDS_1"
  val SPACE="KAAS"
  val TOKEN = ConfluenceToken.getConfluenceToken(TEST_PROPERTIES)
  //val SPACE="~npa_minions"


//  it  ("Pretty Print ClusterInfo") {
//   // val cluster = ClusterInfo.createClusterInfo(session, "LLDS_1")
//    //TODO fix me
//  //  PrettyPrint.prettyPrintKeyspace(cluster,"key2")
//   // assert (true)
//  }

  //Generate the confluence pages
  it  ("Generate Confluence - Sequenctially") {
    val allClusters = ClusterInfo.createClusterInfo(session, GROUP)
    GenerateCassandraConfluencePages.generateAllConfluencePages (allClusters, SPACE ,GROUP, TOKEN, false)
  }


  it  ("Confluence Test Read Page") {
    val token: Token = ConfluenceToken.getConfluenceToken (TEST_PROPERTIES)
    //val pageName = "Test - D3.js chart"
    val pageName = "<Keyspace Name>"

    val page: Page = new Page
    val parentPage: RemotePage = page.read(SPACE, pageName)
    println (parentPage.getContent)
  }

  // intellij reports an incorrect error here due to 'seconds'
  it("AKKA - generate Confluence pages") {
    case object Finished
    val controller = system.actorOf(Props[Controller])

    val actorRef = TestActorRef(new Actor {
      def receive = {
        case done: Done => {
          println ("DONE!!!")
          GenerateCassandraConfluencePages.generateAllConfluencePages (done.allClusters, SPACE, GROUP, TOKEN, false )
          //reply to testActor to keep test running until finished!!!
          testActor ! Finished
        }
      }
    })
    controller ! GetClusterGroup (actorRef, GROUP, session)
    import scala.concurrent.duration._
    expectMsg(600 seconds,Finished)
    //TODO shutdown system
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



}



