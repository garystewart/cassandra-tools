package ganda

//import nl.ing.cassandra.util.MapTypes

import scala.xml.NodeBuffer

object GenerateConfluencePage {
  private val CONFLUENCE_TABLE_START: String = "<table><tbody><tr><th>Table Name</th><th colspan=\"3\">Columns</th><th>Comments</th><th>References</th><th>Properties</th></tr>"
  private val CONFLUENCE_TABLE_END: String = "</tbody></table>"
  private val CONFLUENCE_BR: String = "xxxxxxx"
  private val TABLE_REPLACE: String = "myTable"
  private val KEYSPACE_REPLACE: String = "myKeyspace"


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

  def generateConfluencePage(clusterInfo: ClusterInfo, k: String): String= {
    template.replace(TABLE_REPLACE,
      CONFLUENCE_TABLE_START +
        clusterInfo.keyspaces.filter(_.keyspace_name == k).foldLeft("") { (a, keyspace) =>
          a + keyspace.tables.foldLeft("") { (at, table) =>
            at +  tableRow (table, keyspace)
        }
        } + CONFLUENCE_TABLE_END
    ).replace(CONFLUENCE_BR,"<br/>").replace(KEYSPACE_REPLACE,k)
  }


  val template: String = <body><ac:structured-macro ac:name="warning">
    <ac:parameter ac:name="title">GENERATED CODE!!!</ac:parameter> <ac:rich-text-body>
      <p>BELOW TABLE IS GENERATED FROM C*.<br/>
        PLEASE DON'T CHANGE MANUALLY.</p>
    </ac:rich-text-body>
  </ac:structured-macro>
    <hr/><h1>Keyspace: myKeyspace</h1><p><br/>myTable</p></body>
    .toString
}
