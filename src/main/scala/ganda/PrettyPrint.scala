package eu.ganda

object PrettyPrint {

  def prettyPrintColumn(c : Column): String = {
    c.column_name + " " + c.dataType
  }

  def prettyPrintKeyspace (clusterInfo: ClusterInfo, k: String): Unit = {
    clusterInfo.keyspaces.filter(_.keyspace_name == k).foreach(k => {
      println ("KEYSPACE      : " + k.keyspace_name)
      k.tables.foreach(t => {
        println (" TABLE           : " + t.table_name)
        println ("  PK             : " + t.pkColumns.foldLeft(""){(a, column) => a + (if (!a.isEmpty ) ", " else "") + prettyPrintColumn(column)  })
        println ("  CK             : " + t.ckColumns.foldLeft(""){(a, column) => a + (if (!a.isEmpty ) ", " else "") + prettyPrintColumn(column)  })
        println ("  REGULAR        : " + t.regularColumns.foldLeft(""){(a, column) => a + (if (!a.isEmpty ) ", " else "") + prettyPrintColumn(column)  })
        println ("  Statements     : ")
        t.statements.foreach(s => println("    " + s))
      }
      )
      println ("  Possible Links : ")
      k.findPossibleLinks.foreach(s => println("    " + s))
      println()
    }
    )
  }
}
