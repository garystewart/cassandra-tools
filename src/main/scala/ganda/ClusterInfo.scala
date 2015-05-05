package ganda

import com.datastax.driver.core._
import scala.collection.JavaConversions._

case class Check (check: String, hasPassed: Boolean, severity: String )

case class Column(columnMetadata: ColumnMetadata, keyType: String) {
  val keyspace_name = columnMetadata.getTable.getKeyspace
  val table_name    = columnMetadata.getTable.getName
  val column_name   = columnMetadata.getName

  val dataType      = columnMetadata.getType.getName.toString match {
    case "list" => s"list <${columnMetadata.getType.getTypeArguments.get(0)}>"
    case "map"  => s"map <${columnMetadata.getType.getTypeArguments.get(0)}, ${columnMetadata.getType.getTypeArguments.get(1)}>"
    case "set"  => s"set <${columnMetadata.getType.getTypeArguments.get(0)}>"
    case _      =>  columnMetadata.getType.getName.toString
  }
  val dataTypeLong  = dataType + (if (columnMetadata.isStatic){ " STATIC"} else {""})
  val index_name    = if (columnMetadata.getIndex != null) {columnMetadata.getIndex.getName} else {""}


  val checks: List[Check] = {
    List(
      //check if secondary index exists on table
      Check(s"${index_name} index on table ${table_name}.${column_name}!", index_name == "", "warning" ),
      //check if column names created are not lowercase
      Check(s"${table_name}.${column_name} is not lower case!", column_name.equals(column_name.toLowerCase()), "warning" )
    )
  }
}

case class Table(tableMetadata: TableMetadata) {

  val keyspace_name                 = tableMetadata.getKeyspace.getName
  val table_name                    = tableMetadata.getName
  val comments                      = tableMetadata.getOptions.getComment
  val cql                           = tableMetadata.exportAsString
  val pkColumns: List[Column]       = tableMetadata.getPartitionKey.foldLeft(List[Column]()){(a, p) => a ++ List(Column(p, "partition_key" ))}
  val ckColumns: List[Column]       = tableMetadata.getClusteringColumns.foldLeft(List[Column]()){(a, p) => a ++ List(Column(p, "clustering_key" ))}
  val regularColumns: List[Column]  = tableMetadata.getColumns.filter(c => !tableMetadata.getPrimaryKey.contains(c)).foldLeft(List[Column]()){(a, p) => a ++ List(Column(p, "regular" ))}
  val columns                       = { pkColumns ++ ckColumns ++ regularColumns }

  val insertStatement: String ={
    val colList = columns.foldLeft(""){(a, column) => a + (if (!a.isEmpty ) ", " else "") + column.column_name  }
    val valuesList = columns.foldLeft(""){(a, column) => a + (if (!a.isEmpty ) ", " else "") + "?"}
    s"INSERT INTO $table_name ($colList) VALUES ($valuesList);"
  }

  val deleteStatements = ckColumns.inits.map(pkColumns ++ _).toList.foldLeft(List[String]()){(acc, col) =>
    val whereList = col.foldLeft("") { (a, col2) => a + (if (!a.isEmpty) " AND " else "") + col2.column_name + " = ?" }
    //println (whereList)
    val delStmt = s"DELETE FROM $table_name WHERE $whereList;"
    acc ++ List(delStmt)
  }

  val selectStatements = ckColumns.inits.map(pkColumns ++ _).toList.foldLeft(List[String]()){(acc, col) =>
    val whereList = col.foldLeft("") { (a, col2) => a + (if (!a.isEmpty) " AND " else "") + col2.column_name + " = ?" }
    //println (whereList)
    val selStmt: String = s"SELECT * FROM $table_name WHERE $whereList;"
    acc ++ List(selStmt)
  }

  val statements = {selectStatements ++ List(insertStatement) ++ deleteStatements}


  //TODO extra table checks
  val checks: List[Check] = {
    val tableChecks = List(
      Check(s"${table_name} only has a single column!", columns.size != 1, "warning" )
    )

    //Add table and column checks
    columns.foldLeft(tableChecks){(acc, col) => acc ++ col.checks.map(ch => Check(s"${ch.check}", ch.hasPassed, ch.severity)) }
  }

}


case class Link (from: Table, to: Table, on: String)

case class Keyspace (keyspaceMetaData: KeyspaceMetadata) {

  val keyspace_name = keyspaceMetaData.getName
  val schemaScript = keyspaceMetaData.exportAsString()
  val tables: List[Table] = keyspaceMetaData.getTables.foldLeft(List[Table]()){(a, t)=> a ++ List(Table(t) )}
  //val schemaAgreement = keyspaceMetaData.checkSchemaAgreement()



  //for each table check if link (pks) exists in another table
  val findPossibleLinks: List[Link] = tables.foldLeft(List(): List[Link]){ (acc, t) =>
    acc ++ tables.filter( a => a.table_name != t.table_name).
      foldLeft(List(): List[Link]){(a1, t1) =>
      val a = t1.columns.map(_.column_name).toSet
      val checkLink = a ++ t.pkColumns.map(_.column_name) == a  //if we add the list of pk to a set and the set remains the same then we have a potential link!
      //val s = s" Link ${t1.table_name} to ${t.table_name} on (${t.pkColumns.foldLeft(""){(a, col) => a + (if (!a.isEmpty ) ", " else "") + col.column_name }}) $checkLink"
      if (checkLink) {
        val l = new Link(t1, t , t.pkColumns.foldLeft(""){(a, col) => a + (if (!a.isEmpty ) ", " else "") + col.column_name } )
        a1 ++ List(l)}
      else
        a1
    }
  }


  val ignoreKeyspaces: Set[String] = Set("system_auth","system_traces","system","dse_system")
  //TODO check for existnace of tables!
  val checks: List[Check] = {
    val keyspaceChecks = List(
    //check if keyspace is being used - TODO ignore cassandraerrors and mutations!
      Check(s"No tables in keysapce:$keyspace_name!", tables.size > 0, "warning" ),
    //check for CassandraErrors table
      Check("CassandraErrors table does not exist!", ignoreKeyspaces.contains(keyspace_name) || tables.count(t=> t.table_name.equals("cassandraerrors")) != 0 , "warning" )
    //TODO check for mutations table
    )
    tables.foldLeft(keyspaceChecks){(acc, tab) => acc ++ tab.checks.map(ch => Check(s"${ch.check}", ch.hasPassed, ch.severity)) }
  }
}

//TODO - sort data where makes sense

case class ClusterInfo(metaData: Metadata ) {
  val cluster_name              = metaData.getClusterName
  val keyspaces: List[Keyspace] = metaData.getKeyspaces.map( i => { new Keyspace(i) }).toList
  val schemaAgreement           = metaData.checkSchemaAgreement()
  //val dataCenter
  //TODO - make nice table of HOST info
  val hosts                     = metaData.getAllHosts.map( h => h.getAddress.getHostName + " C* version " + h.getCassandraVersion)
  //
  //TODO add cluster checks summary  ie check DC names etc!
  //TODO implement compare keyspaces - one cluster to another

  val checks: List[Check] = {
    val clusterChecks = List(
      Check(s"Cluster schema agreement issues!",schemaAgreement, "warning" )
    )

    keyspaces.foldLeft(clusterChecks){(acc, col) => acc ++ col.checks.map(ch => Check(s"${ch.check}", ch.hasPassed, ch.severity)) }
  }
}


case class AllClusters (clusterInfoList: List[ClusterInfo]) {

  val checks: List[Check] = {
    val clusterChecks = List(
      Check(s"FIXME!",true, "warning" )
    )
    clusterInfoList.foldLeft(clusterChecks){(acc, clust) => acc ++ clust.checks.map(ch => Check(s"${ch.check}", ch.hasPassed, ch.severity)) }
  }
}


object ClusterInfo {

  def createClusterInfo(session: Session): AllClusters =  {
    val clusterRes = session.execute(new SimpleStatement("select * from cluster where hcpk='hcpk'"))

    val clusterList=
      clusterRes.foldLeft(List[ClusterInfo]()) { (a, row) =>

      val cluster_name = row.getString("cluster_name")
      val uname = row.getString("uname")
      val pword = row.getString("pword")
      val hosts = row.getString("hosts").split(",")
      //TODO add error handling and make reactive!
      lazy val clusSes: Session =
        Cluster.builder().
          addContactPoints(hosts: _*).
          withCompression(ProtocolOptions.Compression.SNAPPY).
          withCredentials(uname, pword).
          //withPort(port).
          build().
          connect()
      val clusterInfo = List(ClusterInfo(clusSes.getCluster.getMetadata))
      clusSes.close()

      a ++  clusterInfo
    }
    AllClusters(clusterList)
  }
}


