package ganda

import com.datastax.driver.core.{Host, Session}
import nl.ing.confluence.rpc.soap.actions.{Page, Token}
import nl.ing.confluence.rpc.soap.beans.RemotePage
import scala.xml.NodeSeq


object GenerateCassandraConfluencePages {

  private val CONFLUENCE_WARNING =  <ac:structured-macro ac:name="warning">
                                      <ac:parameter ac:name="title">GENERATED CODE!!!</ac:parameter>
                                      <ac:rich-text-body><p>NB!!! This page is generated based on information from Cassandra. PLEASE DON'T EDIT IT!!!</p></ac:rich-text-body>
                                    </ac:structured-macro>


  def generateKeyspacePage(keyspace: Keyspace): String= {

    def tableKeyspaceRow (table: Table, k: Keyspace): String = {
      val size = table.columns.size.toString
      //TODO implement SORTING!!!
      val possibleLinks = k.findPossibleLinks.filter(l => l.from.table_name.equals(table.table_name)).foldLeft(""){(a,p) => a + p.to.table_name + " on (" + p.on +")\n" }
      val queries = table.statements.foldLeft(""){(a,s) => a + s + "\n" }
      val tableWarnings = table.checks.filter(!_.hasPassed).foldLeft(""){(a,w) => a + w.check + "\n" }

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

      //first row is needed due to rowSpan setting (see th element in table header)
      val firstRow =
        <tr>
          <td rowspan={size}>{table.table_name}{ Confluence.confluenceCodeBlock("Warnings",tableWarnings,"none")}</td>
          <td class={whichColourClass(table.columns.head.keyType)}>{ table.columns.head.column_name }{whichkeyType(table.columns.head.keyType)}</td>
          <td class={whichColourClass(table.columns.head.keyType)}>{ table.columns.head.dataTypeLong}</td>
          <td rowspan={size}>
            { Confluence.confluenceCodeBlock("CQL",table.cql,"sql")}
            { Confluence.confluenceCodeBlock("Queries",queries,"sql")}
            { Confluence.confluenceCodeBlock("References",possibleLinks,"none")}
            { Confluence.confluenceCodeBlock("Comments",table.comments,"none")}
          </td>
        </tr>

      val restRows = table.columns.tail.foldLeft(""){(a,c) => a +
        <tr>
          <td class={whichColourClass(c.keyType)}>{ c.column_name }{whichkeyType(c.keyType)}</td>
          <td class={whichColourClass(c.keyType)}>{ c.dataTypeLong}</td>
        </tr>
      }

      firstRow + restRows
    }

    val keyspaceWarnings = keyspace.checks.filter(!_.hasPassed).foldLeft(""){(a,w) => a + w.check + "\n" }
    //The actual keyspace page itself
    //need <body> tag otherwise ArrayBuilder is shown on confluence
    <body>{CONFLUENCE_WARNING}<hr/>
      <h1>Keyspace: {keyspace.keyspace_name}</h1>
      <p>{ Confluence.confluenceCodeBlock("Warnings", keyspaceWarnings ,"none")}</p>
      <p>{ Confluence.confluenceCodeBlock("Schema",keyspace.schemaScript,"none")}</p>
      <h1>Tables</h1>
      <p>
        <table>
          <tbody><tr><th>Table Name</th><th colspan="2">Columns</th><th>Extras</th></tr>
            {scala.xml.Unparsed( keyspace.tables.foldLeft("") { (at, table) => at +  tableKeyspaceRow (table, keyspace)} )}
          </tbody>
        </table>
      </p>
    </body>.toString()
  }


  def generateClusterPage(project: String, clusterInfo: ClusterInfo): String= {
    //TODO CHECK if keyspaces no longer exist!!
    //TODO add summary of cluster information
    def clusterRow (clusterInfo: ClusterInfo): String = {
      val rows = clusterInfo.keyspaces.foldLeft(""){(a,k) =>
        //val keyProp = k.properties.foldLeft(""){(a,p) => a + p + "\n" }
        val warnings = k.checks.filter(!_.hasPassed).foldLeft(""){(a,w) => a + w.check + "\n" }

        val href = s"/display/$project/${clusterInfo.cluster_name.replace(" ","+")}+-+${k.keyspace_name}"
          a +
          <tr>
            <td><a href={href}>{k.keyspace_name}</a>{ Confluence.confluenceCodeBlock("Warnings",warnings,"scala")}</td>
            <td>
      <!--        { Confluence.confluenceCodeBlock("Properties",keyProp,"scala")}-->
              { Confluence.confluenceCodeBlock("Schema",k.schemaScript,"none")}
            </td>
          </tr>
        }
      rows
    }
    val clusterWarnings = clusterInfo.checks.filter(!_.hasPassed).foldLeft(""){(a,w) => a + w.check + "\n" }
    val allHosts: String = clusterInfo.hosts.foldLeft(""){(a,h) => a + h + "\n" }

    //The actual cluster page itself
    //need <body> tag otherwise ArrayBuilder is shown on confluence
      <body>{CONFLUENCE_WARNING}<hr/>
        <h1>Cluster: {clusterInfo.cluster_name}</h1>
        <p>{ Confluence.confluenceCodeBlock("All Hosts", allHosts ,"none")}</p>
        <p>{ Confluence.confluenceCodeBlock("Warnings", clusterWarnings ,"none")}</p>
        <h1>Keyspaces</h1>
        <p>
          <table>
            <tbody><tr><th>Keyspace Name</th><th>Extras</th></tr>
              {scala.xml.Unparsed( clusterRow (clusterInfo) )}
            </tbody>
          </table>
        </p>
      </body>.toString
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
    for(k <- clusterInfo.keyspaces)
      yield {
        val content: String = generateKeyspacePage(k)
        val keyPageName =  clusterInfo.cluster_name.toUpperCase + " - " + k.keyspace_name.toUpperCase
        Confluence.confluenceCreatePage (project,keyPageName, content, page, clusterParentPage )
      }
  }
}
