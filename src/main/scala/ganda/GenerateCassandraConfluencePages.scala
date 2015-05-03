package ganda

import com.datastax.driver.core.Session
import nl.ing.confluence.rpc.soap.actions.{Page, Token}
import nl.ing.confluence.rpc.soap.beans.RemotePage

import scala.xml.NodeSeq

/* MD5 object and hash def to prevent updating the page if there are no changes
 */
object MD5 {
  def hash(s: String) = {
    val m = java.security.MessageDigest.getInstance("MD5")
    val b = s.getBytes("UTF-8")
    m.update(b, 0, b.length)
    new java.math.BigInteger(1, m.digest()).toString(16)
  }
}

object Confluence {

  def confluenceCreatePage (project: String, pageName: String, content: String, pageObject: Page, parentPage: RemotePage) : Unit = {
    val contentMD5 = "MD5:" + MD5.hash(content)
    val finalContent = content + "<br/>" + contentMD5
    //println(finalContent)
    try {
      //TODO cluster names is required in name!
      val existingPage: RemotePage = pageObject.read(project,pageName)
      if (!existingPage.getContent.toString.contains(contentMD5) ) {
        println (s"$pageName page updated.")
        existingPage.setContent( finalContent)
        pageObject.store(existingPage)
      } else {
        println (s"$pageName page not updated!")
      }
    }
    catch {
      case e: Exception => {
        val newPage: RemotePage = new RemotePage
        newPage.setContent(finalContent)
        newPage.setTitle( pageName)
        newPage.setParentId(parentPage.getId)
        newPage.setSpace(parentPage.getSpace)
        pageObject.store(newPage)
        println (s"$pageName created!")
      }
    }
  }

//https://confluence.atlassian.com/display/DOC/Code+Block+Macro
  def confluenceCodeBlock (title: String, data: String, lang: String): NodeSeq={
  if (!data.trim.isEmpty) {
    <ac:structured-macro ac:name="code">
      <ac:parameter ac:name="title">{title}</ac:parameter>
      <ac:parameter ac:name="theme">Default</ac:parameter>
      <ac:parameter ac:name="linenumbers">false</ac:parameter>
      <ac:parameter ac:name="language">{lang}</ac:parameter>
      <ac:parameter ac:name="firstline"></ac:parameter>
      <ac:parameter ac:name="collapse">true</ac:parameter>
      <ac:plain-text-body>{ scala.xml.Unparsed("<![CDATA[%s]]>".format(data))}</ac:plain-text-body>
    </ac:structured-macro>
  } else {NodeSeq.Empty}
}


}


object GenerateCassandraConfluencePages {
  private val CONFLUENCE_BR: String = "XXXBRXXX"       //solves issue of linefeed and <br/> between xml in scala and uploading to confluence
  private val TABLE_REPLACE: String = "XXXmyTableXXX"
  private val CONFLUENCE_WARNING =  <ac:structured-macro ac:name="warning">
                                      <ac:parameter ac:name="title">GENERATED CODE!!!</ac:parameter>
                                      <ac:rich-text-body><p>NB!!! This page is generated based on information from Cassandra. PLEASE DON'T EDIT IT!!!</p></ac:rich-text-body>
                                    </ac:structured-macro>


  def generateKeyspacePage(clusterInfo: ClusterInfo, keyspace_name: String): String= {
    val CONFLUENCE_TABLE_START: String = "<table><tbody><tr><th>Table Name</th><th colspan=\"2\">Columns</th><th>Extras</th></tr>"
    val CONFLUENCE_TABLE_END: String = "</tbody></table>"

    def tableKeyspaceRow (table: Table, k: Keyspace): String = {
      //not sure why i need this!  seems a version 1.2 difference!
      //if (table.columns.size > 0) {
        val size = table.columns.size.toString

        val possibleLinks = k.findPossibleLinks.filter(_.from.table_name.equals(table.table_name)).foldLeft(""){(a,p) => a + p.to.table_name + " on (" + p.on +")\n" }
        val tabProp = table.properties.foldLeft(""){(a,p) => a + p + "\n" }
        val queries = table.statements.foldLeft(""){(a,s) => a + s + "\n" }
        val warnings = table.warnings.foldLeft(""){(a,w) => a + w._1 + "\n" }


        def whichColourClass(keyType: String): String = keyType match  {
            case "partition_key" => "highlight-green confluenceTd"
            case "clustering_key" => "highlight-blue confluenceTd"
            case _ =>"highlight-yellow confluenceTd"
          }

        def whichkeyType(keyType: String): String = keyType match  {
          case "partition_key" => " (pk)"
          case "clustering_key" => " (ck)"
          case _ =>""
        }

        val firstRow =
          <tr>
            <td rowspan={size}>{table.table_name}{ Confluence.confluenceCodeBlock("Warnings",warnings,"none")}</td>
            <td class={whichColourClass(table.columns.head.keyType)}>{ table.columns.head.column_name }{whichkeyType(table.columns.head.keyType)}</td>
            <td class={whichColourClass(table.columns.head.keyType)}>{ table.columns.head.dataType}</td>
            <!--<td class={whichColourClass(table.columns.head.keyType)}>{ table.columns.head.keyType }</td>-->
            <!--<td rowspan={size}>{ table.comments }</td>-->
            <!--<td rowspan={size}>{ possibleLinks }</td>-->
            <td rowspan={size}>
              { Confluence.confluenceCodeBlock("References",possibleLinks,"none")}
              { Confluence.confluenceCodeBlock("Properties",tabProp,"scala")}
              { Confluence.confluenceCodeBlock("Queries",queries,"sql")}
              { Confluence.confluenceCodeBlock("Comments",table.comments,"none")}
            </td>
          </tr>

        val restRows = table.columns.tail.foldLeft(""){(a,c) => a +
          <tr>
            <td class={whichColourClass(c.keyType)}>{ c.column_name }{whichkeyType(c.keyType)}</td>
            <td class={whichColourClass(c.keyType)}>{ c.dataType}</td>
            <!--<td class={whichColourClass(c.keyType)}>{ c.keyType }</td>-->
          </tr>}

        firstRow + restRows
      //not sure why i need this!
    //  } else ""
    }

    //need <body> tag otherwise ArrayBuilder is shown on confluence
    val template: String = <body>{CONFLUENCE_WARNING}
      <hr/><h1>Keyspace: {keyspace_name}</h1><p><br/>{TABLE_REPLACE}</p></body>
      .toString()

    template.replace(TABLE_REPLACE,
      CONFLUENCE_TABLE_START +
        clusterInfo.keyspaces.filter(_.keyspace_name == keyspace_name).foldLeft("") { (a, keyspace) =>
          a + keyspace.tables.foldLeft("") { (at, table) =>
            at +  tableKeyspaceRow (table, keyspace)
        }
        } + CONFLUENCE_TABLE_END
    ).replace(CONFLUENCE_BR,"<br/>")


  }

  def generateClusterPage(project: String, clusterInfo: ClusterInfo): String= {
    val CONFLUENCE_TABLE_START: String = "<table><tbody><tr><th>Keyspace Name</th><th>Properties</th></tr>"
    val CONFLUENCE_TABLE_END: String = "</tbody></table>"


    def clusterRow (clusterInfo: ClusterInfo): String = {
      val rows = clusterInfo.keyspaces.foldLeft(""){(a,k) =>
        val keyProp = k.properties.foldLeft(""){(a,p) => a + p + "\n" }
        val warnings = k.tables.foldLeft(""){(a,t) => a + t.warnings.foldLeft(""){(a1,w) => t.table_name + "="+ a1+w._1} + "\n" }
//        val warnings = for{
//          k <- clusterInfo.keyspaces
//          t <- k.tables
//          w <- t.warnings
//        } yield w._1 + "=" + w._2 + "\n"

        val href = s"/display/$project/${clusterInfo.cluster_name.replace(" ","+")}+-+${k.keyspace_name}"
        a +
        <tr>
          <td><a href={href}>{k.keyspace_name}</a>{ Confluence.confluenceCodeBlock("Warnings",warnings,"scala")}</td>
          <td>{ Confluence.confluenceCodeBlock("Properties",keyProp,"scala")}</td>
        </tr>}

      rows
    }

    //need <body> tag otherwise ArrayBuilder is shown on confluence
    val template: String = <body>{CONFLUENCE_WARNING}
      <hr/><p><br/>{TABLE_REPLACE}</p></body>
      .toString()

    template.replace(TABLE_REPLACE,
      CONFLUENCE_TABLE_START +
        clusterRow (clusterInfo)+
        CONFLUENCE_TABLE_END
    ).replace(CONFLUENCE_BR,"<br/>")
  }


  def generateAllConfluencePages (project: String, mainPageName: String, session : Session, confluenceUser: String, confluencePassword: String): Unit = {
    val clusterInfo = ClusterInfo.createClusterInfo(session)
    val token: Token = Token.getInstance
    token.initialise(confluenceUser, confluencePassword)
    val page: Page = new Page

    //Find the main Clusters page
    val parentPage: RemotePage = page.read(project, mainPageName)
    val clusterPageName = clusterInfo.cluster_name.toUpperCase
    //create the specific summary cluster page
    Confluence.confluenceCreatePage (project,clusterPageName, generateClusterPage(project, clusterInfo), page, parentPage )
    val clusterParentPage: RemotePage = page.read(project,clusterPageName)

    //Per keyspace create pages
    for(k <- clusterInfo.keyspaces)       //.filter(_.keyspace_name.toLowerCase().equals("meetup")))
      yield {
        val content: String = generateKeyspacePage(clusterInfo,k.keyspace_name)
        val keyPageName =  clusterInfo.cluster_name.toUpperCase + " - " + k.keyspace_name.toUpperCase
        Confluence.confluenceCreatePage (project,keyPageName, content, page, clusterParentPage )
      }
  }
}
