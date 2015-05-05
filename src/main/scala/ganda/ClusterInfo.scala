package ganda

import com.datastax.driver.core._
import scala.collection.JavaConversions._
import scala.collection.SortedSet

case class Check (name: String, details: String, hasPassed: Boolean, severity: String )

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
      Check("Secondary Index exists", s"$index_name index on table $table_name.$column_name!", index_name == "", "warning" ),
      Check("ColumnName is non-lowercase", s"$table_name.$column_name is not lower case!", column_name.equals(column_name.toLowerCase), "warning" )
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


  //TODO extra table checks based on properties
  val checks: List[Check] = {
    val tableChecks = List(
      Check("Single column table", s"$table_name only has one column!", columns.size != 1, "warning" ),
      Check("TableName is non-lowercase", s"$table_name is not lower case!", table_name.equals(table_name.toLowerCase), "warning" )
    )

    //Add table and column checks
    columns.foldLeft(tableChecks){(acc, col) => acc ++ col.checks}
  }
}


case class Link (from: Table, to: Table, on: String)

//TODO nice solution for DC names!
case class Keyspace (keyspaceMetaData: KeyspaceMetadata, private val validDCnames: SortedSet[String]) extends Ordered[Keyspace] {

  def compare(that: Keyspace): Int = this.keyspace_name.toLowerCase compare that.keyspace_name.toLowerCase

  val keyspace_name = keyspaceMetaData.getName
  val schemaScript = keyspaceMetaData.exportAsString()
  val tables: List[Table] = keyspaceMetaData.getTables.foldLeft(List[Table]()){(a, t)=> a ++ List(Table(t) )}
  //TODO make more elegant!
  val dataCenter: SortedSet[String] = keyspaceMetaData.getReplication.filterNot(a => a._1.equals("class")).filterNot(b => b._1.equals("replication_factor")).map(_._1).to

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

  val checks: List[Check] = {
    val keyspaceChecks = List(
    //TODO ignore cassandraerrors and mutations otherwise appears to be used!!
    //TODO check DC names are valid
      Check("Keyspace is unused check" , s"No tables in keyspace: $keyspace_name!", tables.size > 0, "warning" ),
      Check("CassandraErrors table check",s"$keyspace_name has no CassandraErrors table.", ignoreKeyspaces.contains(keyspace_name) || tables.count(t=> t.table_name.equals("cassandraerrors")) != 0 , "warning" ),
      Check("ModelMutation table check",s"$keyspace_name has no ModelMutation table.", ignoreKeyspaces.contains(keyspace_name) || tables.count(t=> t.table_name.equals("modelmutation")) != 0 , "warning" ),
      //DC checks a + b = a  so nothing added
    //TODO remove treeset from DC message!
      Check("Data Center names check",s"$keyspace_name has incorrect DC names: ${dataCenter}",dataCenter ++ validDCnames == validDCnames   , "warning" )
    )
    tables.foldLeft(keyspaceChecks){(acc, tab) => acc ++ tab.checks }
  }
}

//TODO add Cassandra node object
//TODO ask about Sorted objkects etc

case class ClusterInfo(metaData: Metadata ) {
  val cluster_name                   = metaData.getClusterName
  val schemaAgreement                = metaData.checkSchemaAgreement()
  val dataCenter: SortedSet[String]  = metaData.getAllHosts.groupBy(h => h.getDatacenter).keys.to
  val keyspaces: SortedSet[Keyspace] = metaData.getKeyspaces.map( i => { new Keyspace(i,dataCenter) }).to

  //TODO - make nice table of HOST info
  val hosts                     = metaData.getAllHosts.map( h => h.getAddress.getHostName + " C* version " + h.getCassandraVersion)
  //
  //TODO add cluster checks summary  ie check DC names etc!
  //TODO implement compare keyspaces - one cluster to another

  val checks: List[Check] = {
    val clusterChecks = List(
      Check("Cluster agreement check", s"Cluster schema agreement issues!",schemaAgreement, "warning" )
    )
    keyspaces.foldLeft(clusterChecks){(acc, col) => acc ++ col.checks}
  }
}

case class AllClusters (clusterInfoList: List[ClusterInfo]) {

  val checks: List[Check] = {
    val clusterChecks = List(
    //TODO check naming conventions of DC names
      Check("Check DC names TODO",s"FIXME!",true, "warning" )
    )
    clusterInfoList.foldLeft(clusterChecks){(acc, clust) => acc ++ clust.checks}
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