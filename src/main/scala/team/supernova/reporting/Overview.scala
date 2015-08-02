package team.supernova.reporting

import nl.ing.confluence.rpc.soap.actions.Token
import nl.ing.confluence.rpc.soap.actions.Page
import nl.ing.confluence.rpc.soap.beans.RemotePage

import scala.xml.{Node, Elem}


object Overview {

case class KeyspaceCluster (keyspace: String, clusterName: String)

//case class KeyspaceInfo (keyspace: String,
//                           clusterName: String,
//                           var dcabGroup: String)



  //return all the keyspaces name per cluster - TODO perhaps per group
  def getKeyspaces (token: Token) : List[KeyspaceCluster] = {
    var keyspaceInfoList: List[KeyspaceCluster] = List[KeyspaceCluster]()

    val pageName = "Clusters"
    val page: Page = new Page
    val parentPage: RemotePage = page.read("kaas", pageName)
    val groupPages =  token.getService.getChildren(token.getToken, parentPage.getId)

    //group pages e.g. LLDS_1
    for(gPage <- groupPages)
      yield {
        val clusterPages =  token.getService.getChildren(token.getToken, gPage.getId)
        //cluster pages e.g. LLDS_1_DEV
        for(cPage <- clusterPages)
          yield {
            val ksPages =  token.getService.getChildren(token.getToken, cPage.getId)
            //keyspaces pages e.g. LLDS_1_DEV - ftl
            for(ksPage <- ksPages)
              yield {
                //skip archive folder and the -TOKEN page
                if (cPage.getTitle != "Archive" && ksPage.getTitle != gPage.getTitle+ "-TOKEN") {
                  val ksName = ksPage.getTitle.split(" - ")(1).toLowerCase
                  val clusName = ksPage.getTitle.split(" - ")(0)
                  val x = new KeyspaceCluster (ksName,clusName)
                  keyspaceInfoList =  x :: keyspaceInfoList
                }
              }
          }
      }
    keyspaceInfoList
  }

  def getStringFromTable (content : Elem, name : String  ): String = {
    ( content \\ "tr" filter(x => x.child(0).text.contains(name) ) map (a => a.child(1).toString())).headOption.getOrElse("")
  }

  def getTextFromTable (content : Elem, name : String  ): String = {
    ( content \\ "tr" filter(x => x.child(0).text.contains(name) ) map (a => a.child(1).text)).headOption.getOrElse("")
  }

  def getUsersFromValue (content : Elem, name : String): List[Node] = {
    var retVal: List[Node] = List.empty
    val rawUser = getStringFromTable(content, name)
    try {
      val test = scala.xml.XML.loadString("<?xml version=\"1.0\" encoding=\"utf-8\"?>" + "<body xmlns:ri=\"http://ri\">"+ rawUser + "</body>")
      //println ("test: " + test)
      retVal = (test \\ "link").toList //.map(a => List(a))  //.foldLeft(List[Elem])((a,b) => a ++ b)
    //  println ("retval: " + retVal)
    }
    catch {
      case e: Exception => {
        println ("EXCEPTION - getUsersFromValue - " + e)
      }
    }
    println (name + " " + retVal)
    retVal
  }

  def findLinkedKeyspace (content : Elem): String= {
    (content \\ "link" \\ "page" \ "@{http://ri}content-title" filter(a => a.text !=  "Patterns - Use Cases" )).text  //map(a => println(kPage.getTitle +" -> "+a.text))
  }

  def getKeyspacesInfoFromManualPage (token: Token) = {
    val pageName = "Keyspaces"
    val page: Page = new Page
    val parentPage: RemotePage = page.read("kaas", pageName)

    val keyspacePages =  token.getService.getChildren(token.getToken, parentPage.getId)
    for(kPage <- keyspacePages)
      yield {
        val p: Page = new Page
        val childPage = p.read("kaas",kPage.getTitle)

        val content = scala.xml.XML.loadString("<?xml version=\"1.0\" encoding=\"utf-8\"?>" +
          //need to removed nbsp and dashes from names
          "<!DOCTYPE some_name [ \n<!ENTITY nbsp \"&#160;\">\n<!ENTITY ndash \"&#160;\"> \n]> " +
          // body specify namespace to so that it can be used to extract URIs
          "<body xmlns:ri=\"http://ri\">"+ childPage.getContent + "</body>")

        //find linked page!!

        val linkedKeyspace =findLinkedKeyspace(content) // \\ "link" \\ "page" \ "@{http://ri}content-title" filter(a => a.text !=  "Patterns - Use Cases" ) map(a => println(kPage.getTitle +" -> "+a.text))


        //if (kPage.getTitle == "api_directory") {
        if (linkedKeyspace =="") {
          println(kPage.getTitle)
          val applicationName = getTextFromTable(content, "Name of Application/UseCase")
          val dcab = getTextFromTable(content, "DCAB Group")
          val dcabApprover = getUsersFromValue(content, "DCAB Approver")
          val devContacts = getUsersFromValue(content, "Dev contacts(s)")
          val opsContacts = getUsersFromValue(content, "Ops contact(s)")
        }
        else {
          println(kPage.getTitle +" -> "+ linkedKeyspace)
        }

      }
  }


  def generateList (confluenceUser: String, confluencePass: String) : Unit = {
    //logon to confluence
    val token: Token = Token.getInstance
    token.initialise(confluenceUser, confluencePass)

    //find all keyspaces
    val keyspaceInfoList = getKeyspaces (token)
    val keyspaceClusterList = keyspaceInfoList.groupBy(_.keyspace).mapValues(_.map(_.clusterName).toList)
    val clusterKeyspaceList = keyspaceInfoList.groupBy(_.clusterName).mapValues(_.map(_.keyspace).toList)
    //println (keyspaceClusterList)
    //println (clusterKeyspaceList)

    getKeyspacesInfoFromManualPage(token)

    println("done")

  }


}
