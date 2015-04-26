package ganda

object PrettyPrint {

  def prettyPrintColumn(c : Column): String = {
    c.column_name + " " + c.dataType
  }

  def prettyPrintKeyspace (clusterInfo: ClusterInfo, k: String): Unit = {
    clusterInfo.keyspace.filter(_.keyspace_name == k).foreach(k => {
      println ("KEYSPACE      : " + k.keyspace_name)
      k.tables.foreach(t => {
        println (" TABLE        : " + t.table_name)
        println ("  PK          : " + t.pkColumns.foldLeft(""){(a, column) => a + (if (!a.isEmpty ) ", " else "") + prettyPrintColumn(column)  })
        println ("  CK          : " + t.ckColumns.foldLeft(""){(a, column) => a + (if (!a.isEmpty ) ", " else "") + prettyPrintColumn(column)  })
        println ("  REGULAR     : " + t.regularColumns.foldLeft(""){(a, column) => a + (if (!a.isEmpty ) ", " else "") + prettyPrintColumn(column)  })
        //println ("  Insert      : " + t.insertStatement)
        //println ("  Delete      : " + t.deleteStatements)
        //println ("  Select      : " + t.selectStatements)
        println ("  Statements  : ")
        t.statements.foreach(s => println("    " + s))
        println()
      }
      )
    }
    )

  }

}
