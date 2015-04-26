package ganda

import com.datastax.driver.core.ColumnDefinitions.Definition
import com.datastax.driver.core._
import scala.collection.JavaConversions._

//TODO make enums
case class Column(properties: Map[String, String]) {
  def keyspace_name     = properties.getOrElse("keyspace_name", "")
  def table_name        = properties.getOrElse("columnfamily_name", "")
  def column_name       = properties.getOrElse("column_name", "")
  def keyType           = properties.getOrElse("type", "regular")
  def component_index   = properties.getOrElse("component_index", "-1")
  def dataType          = CQL.getValidatorAsCQL ( properties.getOrElse("validator", ""))
  def index_name        = properties.getOrElse("index_name", "")
  def index_options     = properties.getOrElse("index_options", "")
  def index_type        = properties.getOrElse("index_type", "")
}

case class Table(private val inColumns: List[Column], properties: Map[String, String]) {
  def keyspace_name     = properties.getOrElse("keyspace_name", "")
  def table_name        = properties.getOrElse("columnfamily_name", "")

  def pkColumns = { inColumns.filter(c => c.keyType == "partition_key").sortBy(_.component_index) }
  def ckColumns = { inColumns.filter(c => c.keyType == "clustering_key").sortBy(_.component_index)}
  def regularColumns = { inColumns.filter(c => c.keyType == "regular").sortBy(_.component_index) }
  def columns = { pkColumns ++ ckColumns ++ regularColumns }

  def insertStatement: String ={
    "INSERT INTO " + table_name +
      " (" + columns.foldLeft(""){(a, column) => a + (if (!a.isEmpty ) ", " else "") + column.column_name  } +
      ") VALUES (" + columns.foldLeft(""){(a, column) => a + (if (!a.isEmpty ) ", " else "") + "?"} + ");"
  }

  private def deleteStatement(c: List[Column], acc: List[String], stop: Boolean ): List[String] = {
    stop match {
      case false => c match {
        case head :: tail => {
          val delStmt = "DELETE FROM " + table_name + " WHERE " + c.reverse.foldLeft(""){(a, col) => a + (if (!a.isEmpty ) " AND " else "") + col.column_name +" = ?"  } + ";"
          //stop after first column found with partition key
          deleteStatement (tail, acc ++ List (delStmt), head.keyType == "partition_key")
        }
        case _ => acc
      }
      case true => acc
    }
  }

  def deleteStatements: List[String] = { deleteStatement ((pkColumns ++ ckColumns).reverse, List.empty, false) }

  private def selectStatement(c: List[Column], acc: List[String], stop: Boolean ): List[String] = {
    stop match {
      case false => c match {
        case head :: tail => {
          val delStmt = "SELECT * FROM " + table_name + " WHERE " + c.reverse.foldLeft(""){(a, col) => a + (if (!a.isEmpty ) " AND " else "") + col.column_name +" = ?"  } + ";"
          //stop after first column found with partition key
          selectStatement (tail, acc ++ List (delStmt), head.keyType == "partition_key")
        }
        case _ => acc
      }
      case true => acc
    }
  }

  def selectStatements = { selectStatement ((pkColumns ++ ckColumns).reverse, List.empty, false) }
  def statements = {selectStatements ++ List(insertStatement) ++ deleteStatements}
}

case class Keyspace(tables: List[Table], properties: Map[String, String]) {
  def keyspace_name = properties.getOrElse("keyspace_name", "")
}

case class ClusterInfo(keyspace: List[Keyspace]) {
}

object ClusterInfo {

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
    val clusterInfo = ClusterInfo(keyRes.iterator().map(
      i => {
        val kName = i.getString("keyspace_name")
        //only add tables belonging to keyspace
        new Keyspace(tables.filter(f => {
          f.keyspace_name == kName
        }), CQL.getRowAsProperty(i, Set.empty))
      }
    ).toList
    )
    clusterInfo
  }

}

object CQL {
  //TODO make const in upperCamel case
  // TODO make functin to also do map set etc
  //TODO check based on type on text?
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

  def getValidatorAsCQL (validator: String): String = {
    //TODO add lookup table!
    validator.substring(validator.lastIndexOf(".")+1)
  }

  def getRowAsProperty(row: Row, filterName: Set[String]): Map[String, String] = {
    row.getColumnDefinitions.filter(cd => !filterName.contains(cd.getName)).map(p => {
      (p.getName, CQL.getCQLValueAsString(row, p))
    }).toMap
  }
}
