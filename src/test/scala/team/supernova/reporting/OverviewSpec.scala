package team.supernova.reporting

import akka.actor.ActorSystem
import akka.testkit._
import team.supernova.{ConfluenceProp, TestCassandraCluster}
import org.scalatest._


class OverviewSpec extends TestKit(ActorSystem("ClusterInfoSpec"))
 //with DefaultTimeout with ImplicitSender
 with FunSpecLike //with Matchers with BeforeAndAfterAll
 with TestCassandraCluster {

   it  ("Confluence Test gen List") {
     Overview.generateList (ConfluenceProp.getConfluenceToken)
   }


}



