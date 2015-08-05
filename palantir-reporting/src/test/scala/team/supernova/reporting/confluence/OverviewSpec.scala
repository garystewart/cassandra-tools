package team.supernova.reporting.confluence

import org.scalatest._

/**
 * Created by Gary Stewart on 4-8-2015.
 *
 */
class OverviewSpec extends FunSpecLike {

   it  ("Confluence Test gen List") {
     Overview.generateList (ConfluenceToken.getConfluenceToken ("test.properties") )
   }

}



