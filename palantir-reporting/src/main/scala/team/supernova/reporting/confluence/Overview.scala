package team.supernova.reporting.confluence

import team.supernova.confluence.soap.rpc.soap.actions.{Page, Token}
import team.supernova.confluence.soap.rpc.soap.beans.RemotePage
import scala.xml.Elem

/**
 * Created by Gary Stewart on 4-8-2015.
 *
 */

object Overview {

case class KeyspaceCluster (keyspace: String, clusterName: String)

case class KeyspaceInfo (keyspace       : String,
                         clusterName    : String,
                         linkedCluster  : String,
                         dcabGroup      : String,
                         dcabApprover   : List[String],
                         devContacts    : List[String],
                         opsContacts    : List[String])



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

  def getUsersFromValue (token: Token, content : Elem, name : String): List[String] = {
    var retVal: List[String] = List.empty
    val rawUser = getStringFromTable(content, name)
    try {
      val test = scala.xml.XML.loadString("<?xml version=\"1.0\" encoding=\"utf-8\"?>" + "<body xmlns:ri=\"http://ri\">"+ rawUser + "</body>")

      retVal = (test \\ "user" ).map(a => token.getServiceLocator.getConfluenceserviceV2.getUserByKey(token.getToken, (a \ "@{http://ri}userkey").text).getFullname ).toList //.map(a => List(a))  //.foldLeft(List[Elem])((a,b) => a ++ b)
      println ("retval: " + retVal)
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

  def getKeyspaceInfo(token: Token, keyspaceName: String): KeyspaceInfo = {
    val p: Page = new Page
    val childPage = p.read("kaas",keyspaceName)

    val content = scala.xml.XML.loadString("<?xml version=\"1.0\" encoding=\"utf-8\"?>" +
      //need to removed nbsp and dashes from names
      "<!DOCTYPE some_name [ \n<!ENTITY nbsp \"&#160;\">\n<!ENTITY ndash \"&#160;\"> \n]> " +
      // body specify namespace to so that it can be used to extract URIs
      s"""<body xmlns:ri="http://ri">${childPage.getContent}</body>""")


    //find linked page!!
    val linkedKeyspace =findLinkedKeyspace(content)

    var dcab: String = null
    var dcabApprover = List[String]()
    var devContacts = List[String]()
    var opsContacts = List[String]()

    //if (kPage.getTitle == "api_directory") {
    if (linkedKeyspace =="") {
      println(keyspaceName)
      val applicationName = getTextFromTable(content, "Name of Application/UseCase")
      dcab = getTextFromTable(content, "DCAB Group")
      dcabApprover = getUsersFromValue(token, content, "DCAB Approver")
      devContacts = getUsersFromValue(token, content, "Dev contacts(s)")
      opsContacts = getUsersFromValue(token, content, "Ops contact(s)")
    }
    else {
      println(keyspaceName +" -> "+ linkedKeyspace)
    }
    var keyspaceInfo = new KeyspaceInfo(keyspaceName, "",linkedKeyspace,dcab, dcabApprover, devContacts, opsContacts )
    keyspaceInfo

  }


  def getKeyspacesInfoFromManualPage (token: Token): List[KeyspaceInfo] = {
    val pageName = "Keyspaces"
    val page: Page = new Page
    val parentPage: RemotePage = page.read("kaas", pageName)

    val keyspacePages =  token.getService.getChildren(token.getToken, parentPage.getId)

   var keyspaceInfoList : List[KeyspaceInfo] = List[KeyspaceInfo]()
    for(kPage <- keyspacePages)
      yield {
        keyspaceInfoList = getKeyspaceInfo (token, kPage.getTitle) :: keyspaceInfoList
      }
    keyspaceInfoList
  }


  def generateList (token: Token) : Unit = {

    //find all keyspaces
    val keyspaceInfoList = getKeyspaces (token)
    println (keyspaceInfoList.groupBy(_.keyspace).mapValues(_.map(_.clusterName)))
    val keyspaceClusterList = keyspaceInfoList.groupBy(_.keyspace).mapValues(_.map(_.clusterName))
    val clusterKeyspaceList = keyspaceInfoList.groupBy(_.clusterName).mapValues(_.map(_.keyspace))
    println (keyspaceClusterList)
    println (clusterKeyspaceList)

    getKeyspacesInfoFromManualPage(token)
    println("---- FINISHED ---")
  }
}
