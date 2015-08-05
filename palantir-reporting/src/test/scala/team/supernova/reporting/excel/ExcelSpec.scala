package team.supernova.reporting.excel

import org.scalatest._
import team.supernova.reporting.confluence.{ConfluenceToken, Overview}

/**
 * Created by Gary Stewart on 4-8-2015.
 *
 */

class ExcelSpec extends FunSpecLike {

  it  ("Test Create excel sheet") {
    Excel.createSheet (Overview.getKeyspacesInfoFromManualPage (ConfluenceToken.getConfluenceToken ("test.properties") ))
  }

}
