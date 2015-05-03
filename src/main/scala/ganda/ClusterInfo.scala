package ganda

import com.datastax.driver.core.ColumnDefinitions.Definition
import com.datastax.driver.core._
import scala.collection.JavaConversions._

case class Column(properties: Map[String, String]) {
  val keyspace_name     = properties.getOrElse("keyspace_name", "")
  val table_name        = properties.getOrElse("columnfamily_name", "")
  val column_name       = properties.getOrElse("column_name", "")
  val keyType           = properties.getOrElse("type", "regular")
  val component_index   = properties.getOrElse("component_index", "-1")
  //TODO fix datatypes!  Map, List and Set + Reverse and remove TRY from here
  val dataType          =  CQLMapping.mapCQLTypeFromSchemaColumnsTypeString (properties.getOrElse("validator", ""))
  val index_name        = properties.getOrElse("index_name", "")
  val index_options     = properties.getOrElse("index_options", "")
  val index_type        = properties.getOrElse("index_type", "")
}

case class Table(private val inColumns: List[Column], properties: Map[String, String]) {
  val keyspace_name     = properties.getOrElse("keyspace_name", "")
  val table_name        = properties.getOrElse("columnfamily_name", "")
  val comments          = properties.getOrElse("comment", "")
  val pkColumns = { inColumns.filter(c => c.keyType == "partition_key").sortBy(_.component_index) }
  val ckColumns = inColumns.filter(c => c.keyType == "clustering_key").sortBy(_.component_index)
  val regularColumns = { inColumns.filter(c => c.keyType == "regular").sortBy(_.component_index) }
  val columns = { pkColumns ++ ckColumns ++ regularColumns }

  val insertStatement: String ={
    val colList = columns.foldLeft(""){(a, column) => a + (if (!a.isEmpty ) ", " else "") + column.column_name  }
    val valuesList = columns.foldLeft(""){(a, column) => a + (if (!a.isEmpty ) ", " else "") + "?"}
    s"INSERT INTO $table_name ($colList) VALUES ($valuesList);"
  }

  //TODO val foo = s"someStrign $somevar ${someExpr}"
  //val bar = s""" insert into "somefunkyname" """


  val deleteStatements = ckColumns.inits.map(pkColumns ++ _).toList.foldLeft(List[String]()){(acc, col) =>
    val whereList = col.foldLeft("") { (a, col2) => a + (if (!a.isEmpty) " AND " else "") + col2.column_name + " = ?" }
    //println (whereList)
    val delStmt = s"DELETE FROM $table_name WHERE $whereList;"
    acc ++ List(delStmt)
  }

  val selectStatements =
    ckColumns.inits.map(pkColumns ++ _).toList.foldLeft(List[String]()){(acc, col) =>
    val whereList = col.foldLeft("") { (a, col2) => a + (if (!a.isEmpty) " AND " else "") + col2.column_name + " = ?" }
    //println (whereList)
    val selStmt: String = s"SELECT * FROM $table_name WHERE $whereList;"
    acc ++ List(selStmt)
  }

  val statements = {selectStatements ++ List(insertStatement) ++ deleteStatements}
}

case class Link (from: Table, to: Table, on: String)

case class Keyspace(tables: List[Table], properties: Map[String, String]) {
  val keyspace_name = properties.getOrElse("keyspace_name", "")


  //for each table check if link (pks) exists in another table
  val findPossibleLinks2: List[String] = tables.foldLeft(List("")){ (acc, t) =>
      acc ++ tables.filter( a => a.table_name != t.table_name).
        foldLeft(List("")){(a1, t1) =>
          val a = t1.columns.map(_.column_name).toSet
          val checkLink = a ++ t.pkColumns.map(_.column_name) == a  //if we add the list of pk to a set and the set remains the same then we have a potential link!
          val s = s" Link ${t1.table_name} to ${t.table_name} on (${t.pkColumns.foldLeft(""){(a, col) => a + (if (!a.isEmpty ) ", " else "") + col.column_name }}) $checkLink"
          a1 ++ List(s)
      }
    }.filter(_!="").filter(_.contains("true"))  //only show valid links  TDOD make this a case class

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

}

case class ClusterInfo(keyspaces: List[Keyspace], properties: Map[String, String]) {
  val cluster_name     = properties.getOrElse("cluster_name", "")

//TODO implement compare
//  def compare (keyspace1: String, keyspace2: String) {
//     //println ("DIFF:" + keyspace.filter(k => k.keyspace_name==keyspace1)
//    //.tables.filterNot(keyspace.filter(keyspace_name==keyspace2)).tables.toSet))
//  }
}

object ClusterInfo {

  //read information from system keyspace and create ClusterInfo
  def createClusterInfo(session: Session): ClusterInfo =  {
    //columns
    val colRes = session.execute(new BoundStatement(session.prepare("select * from system.schema_columns;")))
    val columns = colRes.iterator().map(
      row => {
        new Column(CQL.getRowAsProperty(row, Set.empty))
      }
    ).toList

    //tables
    val tabRes = session.execute(new BoundStatement(session.prepare("select * from system.schema_columnfamilies;")))
    val tables = tabRes.iterator().map(
      row => {
        //only add tables belonging to keyspace + table
        val name = row.getString("keyspace_name") + "." + row.getString("columnfamily_name")
        new Table(columns.filter(f => {
          name == (f.keyspace_name + "." + f.table_name)
        }), CQL.getRowAsProperty(row, Set.empty))
      }
    ).toList

    //keysapces
    val keyRes = session.execute(new BoundStatement(session.prepare("select * from system.schema_keyspaces;")))
    val localRes = session.execute(new BoundStatement(session.prepare("select * from system.local;")))

    val clusterInfo = ClusterInfo(keyRes.iterator().map(
      i => {
        val kName = i.getString("keyspace_name")
        //only add tables belonging to keyspace
        new Keyspace(tables.filter(f => {
          f.keyspace_name == kName
        }), CQL.getRowAsProperty(i, Set.empty))
      }
    ).toList,
    //TODO there is only one row in local!  might be better to improve
      CQL.getRowAsProperty(localRes.one(), Set.empty)
    )
    clusterInfo
  }
}

object CQL {
  // TODO make functin to also do map set etc
  def getCQLValueAsString(i: Row, p: Definition): String = {
    p.getType.toString match {
      case "varchar" => i.getString(p.getName)
      case "double" => i.getDouble(p.getName).toString
      case "int" => i.getInt(p.getName).toString
      case "boolean" => i.getBool(p.getName).toString
      //case "map"      => i.getMap(p.getName,String, String)
      case _ => "FIXME: " + p.getType.toString
    }
  }

  def getRowAsProperty(row: Row, filterName: Set[String]): Map[String, String] = {
    row.getColumnDefinitions.filter(cd => !filterName.contains(cd.getName)).map(p => {
      (p.getName, CQL.getCQLValueAsString(row, p))
    }).toMap
  }
}