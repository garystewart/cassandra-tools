package ganda

import com.datastax.driver.core.Session
import nl.ing.confluence.rpc.soap.actions.{Page, Token}
import nl.ing.confluence.rpc.soap.beans.RemotePage
import java.security.MessageDigest

object GenerateConfluencePage {
  private val CONFLUENCE_TABLE_START: String = "<table><tbody><tr><th>Table Name</th><th colspan=\"3\">Columns</th><th>Comments</th><th>References</th><th>Properties</th></tr>"
  private val CONFLUENCE_TABLE_END: String = "</tbody></table>"
  private val CONFLUENCE_BR: String = "xxxxxxx"
  private val TABLE_REPLACE: String = "myTable"
  private val KEYSPACE_REPLACE: String = "myKeyspace"


  object MD5 {
    def hash(s: String) = {
      val m = java.security.MessageDigest.getInstance("MD5")
      val b = s.getBytes("UTF-8")
      m.update(b, 0, b.length)
      new java.math.BigInteger(1, m.digest()).toString(16)
    }
  }



  def tableRow (table: Table, k: Keyspace): String = {
    val size = table.columns.size.toString
    //TODO fix possible link - bad design
    val possibleLinks = k.findPossibleLinks.filter(s => s.contains(table.table_name)).foldLeft(""){(a,p) => a + p + CONFLUENCE_BR }  // TODO add carriage returns
    //TODO remove filter from here!! compaction_strategy_class
    val tabProp = table.properties.filter(s => !(s._1.contains("compaction_strategy_class") ||s._1.contains("comparator") ||s._1.contains("comment") || s._1.contains("key_validator"))).foldLeft(""){(a,p) => a + p + CONFLUENCE_BR }  // TODO add carriage returns
    val firstRow =
      <tr>
        <td rowspan={size}>{table.table_name}</td>
        <td>{ table.columns.head.column_name }</td>
        <td>{ table.columns.head.dataType}</td>
        <td>{ table.columns.head.keyType }</td>
        <td rowspan={size}>{ table.comments }</td>
        <td rowspan={size}>{ possibleLinks }</td>
        <td rowspan={size}>{ tabProp }</td>
      </tr>

      val restRows = table.columns.tail.foldLeft(""){(a,c) => a +
        <tr>
          <td>{ c.column_name }</td>
          <td>{ c.dataType}</td>
          <td>{ c.keyType }</td>
        </tr>}

    firstRow + restRows
  }

  def generateConfluenceKeyspacePage(clusterInfo: ClusterInfo, k: String): String= {
    //need <body> tag otherwise ArrayBuilder is shown on confluence
    val template: String = <body><ac:structured-macro ac:name="warning">
      <ac:parameter ac:name="title">GENERATED CODE!!!</ac:parameter> <ac:rich-text-body>
        <p>BELOW TABLE IS GENERATED FROM Cassandra.<br/>
          PLEASE DON'T CHANGE MANUALLY.</p>
      </ac:rich-text-body>
    </ac:structured-macro>
      <hr/><h1>Keyspace: myKeyspace</h1><p><br/>myTable</p></body>
      .toString

    template.replace(TABLE_REPLACE,
      CONFLUENCE_TABLE_START +
        clusterInfo.keyspaces.filter(_.keyspace_name == k).foldLeft("") { (a, keyspace) =>
          a + keyspace.tables.foldLeft("") { (at, table) =>
            at +  tableRow (table, keyspace)
        }
        } + CONFLUENCE_TABLE_END
    ).replace(CONFLUENCE_BR,"<br/>").replace(KEYSPACE_REPLACE,k)
  }



def confluenceCreatePage (project: String, pageName: String, content: String, pageObject: Page, parentPage: RemotePage) : Unit = {
  val contentMD5 = "MD5:" + MD5.hash(content)

  try {
    //TODO cluster names is required in name!
    val existingPage: RemotePage = pageObject.read(project,pageName)
    if (!existingPage.getContent.toString.contains(contentMD5) ) {
      println (s"$pageName page updated.")
      existingPage.setContent( content + contentMD5)
      pageObject.store(existingPage)
    } else {
      println (s"$pageName page not updated!")
    }
  }
  catch {
    case e: Exception => {
      val newPage: RemotePage = new RemotePage
      newPage.setContent(content + contentMD5)
      newPage.setTitle( pageName)
      newPage.setParentId(parentPage.getId)
      newPage.setSpace(parentPage.getSpace)
      pageObject.store(newPage)
      println (s"$pageName created!")
    }
  }
}


  def generateConfluencePages (project: String, session : Session, confluenceUser: String, confluencePassword: String): Unit = {
    val clusterInfo = ClusterInfo.createClusterInfo(session)
    val token: Token = Token.getInstance
    token.initialise(confluenceUser, confluencePassword)

    val page: Page = new Page
    val parentPage: RemotePage = page.read(project, "GEN")
    val clusterPageName = clusterInfo.cluster_name.toUpperCase
    confluenceCreatePage (project,clusterPageName, "FIXME", page, parentPage )
    val clusterParentPage: RemotePage = page.read(project,clusterPageName)


    for(k <- clusterInfo.keyspaces)
      yield {
        val content: String = GenerateConfluencePage.generateConfluenceKeyspacePage(clusterInfo,k.keyspace_name)
        val keyPageName =  clusterInfo.cluster_name.toUpperCase + " - " + k.keyspace_name.toUpperCase
        confluenceCreatePage (project,keyPageName, content, page, clusterParentPage )

      }
  }
}
