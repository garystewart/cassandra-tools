package team.supernova.confluence

import java.util.Calendar

import com.datastax.driver.core.Session
import team.supernova.confluence.soap.rpc.soap.actions.{Page, Token}
import team.supernova.confluence.soap.rpc.soap.beans.RemotePage
import team.supernova.{AllClusters, ClusterInfo, Keyspace, Table}

import scala.collection.SortedSet
/**
 * Created by Gary Stewart on 4-8-2015.
 *
 */
object GenerateCassandraConfluencePages {

  private def CONFLUENCE_HEADER (intro: String) = <ac:structured-macro ac:name="section">
      <ac:rich-text-body>
        <ac:structured-macro ac:name="column">
          <ac:parameter ac:name="width">50%</ac:parameter> <ac:rich-text-body>
          <h1>General information</h1>
          <ac:structured-macro ac:name="warning">
            <ac:parameter ac:name="title">GENERATED CODE!!!</ac:parameter>
            <ac:rich-text-body><p>NB!!! This page is generated based on information from Cassandra. PLEASE DON'T EDIT IT!!!</p></ac:rich-text-body>
          </ac:structured-macro>
          <p>{intro}</p>
        </ac:rich-text-body>
        </ac:structured-macro> <ac:structured-macro ac:name="column">
        <ac:parameter ac:name="width">50%</ac:parameter> <ac:rich-text-body>
          <ac:structured-macro ac:name="panel">
            <ac:parameter ac:name="title">What's on this page</ac:parameter> <ac:parameter ac:name="borderStyle">solid</ac:parameter> <ac:rich-text-body>
            <p>
              <ac:structured-macro ac:name="toc">
                <ac:parameter ac:name="maxLevel">4</ac:parameter>
              </ac:structured-macro>
            </p>
          </ac:rich-text-body>
          </ac:structured-macro>
        </ac:rich-text-body>
      </ac:structured-macro>
      </ac:rich-text-body>
    </ac:structured-macro>

  def generateKeyspacePage(keyspace: Keyspace): String= {

    def tableKeyspaceRow (table: Table, k: Keyspace): String = {
      val size = table.columns.size.toString
      //TODO implement SORTING!!!
      val possibleLinks = k.findPossibleLinks.filter(l => l.from.table_name.equals(table.table_name)).foldLeft(""){(a,p) => a + p.to.table_name + " on (" + p.on +")\n" }
      val queries = table.statements.foldLeft(""){(a,s) => a + s + "\n" }
      val tableWarnings = table.checks.filter(!_.hasPassed).foldLeft(""){(a,w) => a + w.details + "\n" }

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

    val keyspaceWarnings = keyspace.checks.filterNot(_.hasPassed).filter(c => c.severity.equals("warning")).foldLeft(""){(a,w) => a + w.details + "\n" }
    val keyspaceErrors = keyspace.checks.filterNot(_.hasPassed).filter(c => c.severity.equals("error")).foldLeft(""){(a,w) => a + w.details + "\n" }
    //The actual keyspace page itself
    //need <body> tag otherwise ArrayBuilder is shown on confluence
    <body>{CONFLUENCE_HEADER("This section summarises all the keyspace information.")}<hr/>
      <h1>Keyspace: {keyspace.keyspace_name}</h1>
      <p>{ Confluence.confluenceCodeBlock("Errors", keyspaceErrors ,"none")}
        { Confluence.confluenceCodeBlock("Warnings", keyspaceWarnings ,"none")}</p>
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


  def generateClusterInfoPage(project: String, clusterInfo: ClusterInfo): String= {

    //TODO add summary of cluster information
    def clusterRow (clusterInfo: ClusterInfo): String = {
      val rows = clusterInfo.keyspaces.foldLeft(""){(a,k) =>
        val warnings = k.checks.filter(!_.hasPassed).foldLeft(""){(a,w) => a + w.details + "\n" }

        val href = s"/display/$project/${clusterInfo.cluster_name.replace(" ","+")}+-+${k.keyspace_name}"
          a +
          <tr>
            <td><a href={href}>{k.keyspace_name}</a>{ Confluence.confluenceCodeBlock("Warnings",warnings,"scala")}</td>
            <td>
              { Confluence.confluenceCodeBlock("Schema",k.schemaScript,"none")}
            </td>
          </tr>
        }
      rows
    }
    val clusterWarnings = clusterInfo.checks.filterNot(_.hasPassed).filter(c => c.severity.equals("warning")).foldLeft(""){(a,w) => a + w.details + "\n" }
    val clusterErrors = clusterInfo.checks.filterNot(_.hasPassed).filter(c => c.severity.equals("error")).foldLeft(""){(a,w) => a + w.details + "\n" }

    import org.json4s._
    import org.json4s.jackson.JsonMethods._
    import org.json4s.jackson.Serialization.write
    implicit val formats = DefaultFormats

    //The actual cluster page itself
    //need <body> tag otherwise ArrayBuilder is shown on confluence
      <body>{CONFLUENCE_HEADER("This section summarises all the cluster information.")}<hr/>
        <h1>Cluster: {clusterInfo.cluster_name}</h1>
        <p>{ Confluence.confluenceCodeBlock("Errors", clusterErrors ,"none")}
          { Confluence.confluenceCodeBlock("Warnings", clusterWarnings ,"none")}</p>
          <img src={s"http://${clusterInfo.graphite_host}/render?from=-2hours&until=now&width=400&height=250&target=LLDS.Cassandra.${clusterInfo.cluster_name}.requests.mean&target=LLDS.Cassandra.${clusterInfo.cluster_name}.requests.max&target=LLDS.Cassandra.${clusterInfo.cluster_name}.requests.p99&_uniq=0.8728725090622902"}/>
        <h1>Host Information</h1>
        <p>
          <table>
            <tbody><tr><th>Data Center</th><th>Host Name</th><th>IP Address</th><th>C* Version</th><th>Extras</th></tr>
              {scala.xml.Unparsed( clusterInfo.hosts.to[SortedSet].foldLeft("") { (at, host) => at +
                                            <tr>
                                              <td>{host.dataCenter}</td>
                                              <td>{host.canonicalHostName}</td>
                                              <td>{host.ipAddress}</td>
                                              <td>{host.version}</td>
                                              <td>{
                                                val yaml  =  try { pretty(parse( write(host.opsCenterNode)))}
                                                 catch {case e: Exception => ""}
                                                Confluence.confluenceCodeBlock("Yaml",yaml  ,"none")}</td>
                                            </tr>
                                            })
              }
            </tbody>
          </table>
        </p>
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

  //TODO - make graphite checker!!!  Seems to be rather messy
  //TODO show info in governance schema

   //this is using graphana!
   def clusterGraphite(cluster_name: String,graphana_host: String ) = <a href={s"http://${graphana_host}/dashboard/db/cluster-health-per-cluster?Cluster=${cluster_name}"}>Overview</a>

  def generateClusterSummaryPage(allClusters: AllClusters, project: String): String=  {

    val listKeyspace: SortedSet[String] = allClusters.clusterInfoList.flatMap(_.keyspaces).map(_.keyspace_name).to[SortedSet]
    val listClusterName = allClusters.clusterInfoList.map(_.cluster_name).toSet

    def whichColourClassBoolean(isTrue: Boolean): String =  if (isTrue) {"highlight-green confluenceTd"} else {"highlight-red confluenceTd"}

    <body>{CONFLUENCE_HEADER("This section briefly summarises all the clusters information.")}<hr/>
      <h1>Cluster Summary</h1>
      <p>
        <table>
          <tbody><tr><th>Cluster Name</th><th>Metrics</th><th>Total Errors</th><th>Total Warnings</th><th>Extras</th><th>Last Checked</th></tr>
            {scala.xml.Unparsed( allClusters.clusterInfoList.foldLeft("") { (at, clus) =>
            val warnings = clus.checks.filterNot(_.hasPassed).filter(c => c.severity.equals("warning"))
            val errors = clus.checks.filterNot(_.hasPassed).filter(c => c.severity.equals("error"))
            at +
            <tr>
              <td><a href={s"/display/$project/${clus.cluster_name.replace(" ","+")}"}>{clus.cluster_name}</a></td>
              <td>{clusterGraphite(clus.cluster_name, clus.graphana_host)}</td>
              <td>{errors.size}</td>
              <td>{warnings.size}</td>
              <td>{ Confluence.confluenceCodeBlock("Error", errors.foldLeft(""){(a,w) => a + w.details + "\n" } ,"none")}
                { Confluence.confluenceCodeBlock("Warnings", warnings.foldLeft(""){(a,w) => a + w.details + "\n" } ,"none")}
              </td>
              <td>{ Calendar.getInstance.getTime} </td>
            </tr>
            } )
            }
          </tbody>
        </table>
      </p>
      <h1>Cluster Keyspace Summary</h1>
      <p>
        <table>
          <tbody>
            <tr>
              <th>Keyspace Name</th>
              {scala.xml.Unparsed( listClusterName.foldLeft("") { (acc, clust_name) => acc +
              <th>{clust_name}</th> + <th>Total</th>})
              }
            </tr>
            {scala.xml.Unparsed( listKeyspace.foldLeft("") { (acc, key: String) => acc +
            <tr>
              <td>{ key }</td>
              {scala.xml.Unparsed( listClusterName.foldLeft("") { (acc, clust_name) =>
              val isFound = allClusters.clusterInfoList.filter(a => a.cluster_name.equals(clust_name)).flatMap(_.keyspaces).count(k => k.keyspace_name.equals(key)) > 0
              //TODO fix LINKS!! make more reusable
              acc + <td class={whichColourClassBoolean(isFound)}>{ if (isFound) {<a href={s"/display/$project/${clust_name.replace(" ","+")}+-+$key"}>{isFound}</a>} else isFound }</td> +
                    <td class={whichColourClassBoolean(isFound)}>{ if (isFound) {allClusters.clusterInfoList.filter(a => a.cluster_name.equals(clust_name)).flatMap(_.keyspaces).filter(_.keyspace_name == key).head.tables.size.toString} else ""}</td>})
              }
            </tr>
          } )
            }
          </tbody>
        </table>
      </p>
    </body>.toString
  }



  def generateAllConfluencePages (project: String, mainPageName: String, session : Session,
                                  token: Token, deletePages: Boolean): Unit = {

    //login into confluence
    val allClusters = ClusterInfo.createClusterInfo(session, mainPageName)
    val page: Page = new Page

    //Find the main Clusters page
    val parentPage: RemotePage = page.read(project, mainPageName)
    val tokenPageName = mainPageName + "-TOKEN"

    //read token page

    var tokenPage: RemotePage = null
    try {
      println (s"Reading $tokenPageName page")
      tokenPage = page.read(project, tokenPageName)
      println (s"$tokenPageName page found")
    }
    catch {
      case e: Exception => {
        val newPage: RemotePage = new RemotePage
        newPage.setTitle(tokenPageName)
        newPage.setParentId(parentPage.getId)
        newPage.setSpace(parentPage.getSpace)
        page.store(newPage)
        println (s"$tokenPageName page created!")
      }
    }


    //Always update the Cluster page
    parentPage.setContent( s"<body>${generateClusterSummaryPage(allClusters, project)}</body>")
    page.store(parentPage)


    val listKeyspace = allClusters.clusterInfoList.flatMap(_.keyspaces).map(_.keyspace_name).toSet
    val listClusterName = allClusters.clusterInfoList.map(_.cluster_name).toSet
    var tokenContent: String = ""

    //Per ClusterInfo - create page
    for(cl <- allClusters.clusterInfoList)
      yield {
        val clusterPageName = cl.cluster_name.toUpperCase
        //create the specific summary cluster page
        tokenContent = tokenContent +"<br/>"+ Confluence.confluenceCreatePage (project,clusterPageName, generateClusterInfoPage(project, cl), page, parentPage, tokenPage )
        val clusterParentPage: RemotePage = page.read(project,clusterPageName)

        //Per keyspace create pages
        for(k <- cl.keyspaces)
          yield {
            val content: String = generateKeyspacePage(k)
            //SEE DELETE below if you change this!!!!!!
            val keyPageName =  cl.cluster_name.toUpperCase + " - " + k.keyspace_name.toUpperCase
            tokenContent = tokenContent +"<br/>"+ Confluence.confluenceCreatePage (project,keyPageName, content, page, clusterParentPage, tokenPage )
          }
      }


    //update the TOKEN page
    tokenPage.setContent(tokenContent)
    page.store(tokenPage)
    println (s"TOKEN page updated!")




   //clean up pages no longer needed - ie keyspace deleted
    //TODO add this method to confluence package??
     val clusterPages =  token.getService.getChildren(token.getToken, parentPage.getId)
    //clusterPages.foreach(p => println (p.getTitle))
    def substringAfter(s:String,k:String) = { s.indexOf(k) match { case -1 => ""; case i => s.substring(i+k.length)  } }
    //start bottom up
    for(cPage <- clusterPages)
      yield {
        val keyspacePages =  token.getService.getChildren(token.getToken, cPage.getId)
        for(kPage <- keyspacePages)
          yield {
            //SEE CREATE if you change this!!!!!!
            //Delete keyspace page if not exists
            if (allClusters.clusterInfoList.filter(cList => cList.cluster_name.toUpperCase.equals(cPage.getTitle)).
              flatMap(cl => cl.keyspaces).count(k => k.keyspace_name.toUpperCase.equals(substringAfter(kPage.getTitle," - "))) == 0 )
              {
                println (s"DELETING page($deletePages): ${kPage.getTitle}")
                if (deletePages) {
                  page.remove(kPage.getId)
                  println (s"DELETED page: ${kPage.getTitle}")
                }
              }
          }

//TODO - REMOVED for now - need to rethink if this is handy
//        //Delete cluster page if not exists
//        if (!listClusterName.contains(cPage.getTitle))
//        {
//          println (s"DELETING page: ${cPage.getTitle}")
//          page.remove(cPage.getId)
//        }
      }
  }

}
