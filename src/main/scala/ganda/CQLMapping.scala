package ganda

object CQLMapping {
  private val SCHEMA_PACKAGE: String = "org.apache.cassandra.db.marshal."

  def mapCQLTypeFromSchemaColumnsTypeString(sDataType: String): String = {
    val ret = sDataType match {
      case dt if dt.contains(SCHEMA_PACKAGE + "ReversedType")  =>
        s"${mapCQLTypeFromSchemaColumnsTypeString(dt.replace(SCHEMA_PACKAGE + "ReversedType(", "").replace(")",""))} desc"
      case dt if dt.equals(SCHEMA_PACKAGE + "UTF8Type")  => "varchar"
      case dt if dt.equals(SCHEMA_PACKAGE + "BytesType")  => "blob"
      case dt if dt.equals(SCHEMA_PACKAGE + "Int32Type")  => "int"
      case dt if dt.equals(SCHEMA_PACKAGE + "LongType")  => "bigint"
      case dt if dt.contains(SCHEMA_PACKAGE + "MapType")  => {
        val values: String = dt.replace(SCHEMA_PACKAGE + "MapType(", "").replace(")","").
          split(",").foldLeft(""){(acc, s) => { acc + (if (!acc.isEmpty ) ", " else "") +  mapCQLTypeFromSchemaColumnsTypeString(s)}}
         s"map <${values}>"
      }
      case dt if dt.contains(SCHEMA_PACKAGE + "SetType")  =>
        s"set <${mapCQLTypeFromSchemaColumnsTypeString(dt.replace(SCHEMA_PACKAGE + "SetType(", "").replace(")",""))}>"
      case dt if dt.contains(SCHEMA_PACKAGE + "ListType")  => "bigint"
        s"list <${mapCQLTypeFromSchemaColumnsTypeString(dt.replace(SCHEMA_PACKAGE + "ListType(", "").replace(")",""))}>"
      case dt if dt.contains(SCHEMA_PACKAGE + "")  => sDataType.replace(SCHEMA_PACKAGE, "").toLowerCase().replace("type","")
      case _ => { println (sDataType + "$dt not solved!!!!!")
        ""}
    }
//    println (s"$sDataType = $ret")
    ret
  }
}

