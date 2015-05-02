package ganda

object SchemaTypes extends Enumeration {
  val AbstractCommutativeType, AbstractCompositeType, AbstractType, AsciiType, BooleanType, BytesType, CollectionType, ColumnToCollectionType, CompositeType, CounterColumnType, DateType, DecimalType, DoubleType, DynamicCompositeType, EmptyType, FloatType, InetAddressType, Int32Type, IntegerType, LexicalUUIDType, ListType, LocalByPartionerType, LongType, MapType, ReversedType, SetType, TimeUUIDType, TimestampType, TypeParser, UTF8Type, UUIDType = Value
}

//TODO FIX FOR MAP types!!!!
object Mapping {
  private val SCHEMA_PACKAGE: String = "org.apache.cassandra.db.marshal."


  def mapCQLTypeFromSchemaColumnsTypeString(sDataType: String): String = {
    //println ("DataType: " + sDataType)
    val schemas = SchemaTypes.withName(sDataType.replace(SCHEMA_PACKAGE, ""))
    //println ("Schema: " + schemas)

    schemas match {
      case SchemaTypes.AbstractCommutativeType => "abstractcommutative"
      case SchemaTypes.AbstractCompositeType => "abstractcomposite"
      case SchemaTypes.AbstractType => "abstract"
      case SchemaTypes.AsciiType =>    "ascii"
      case SchemaTypes.BooleanType =>   "boolean"
      case SchemaTypes.BytesType =>        "blob"
      case SchemaTypes.CollectionType => "collection"
      case SchemaTypes.ColumnToCollectionType => "columntocollection"
      case SchemaTypes.CompositeType => "composite"
      case SchemaTypes.CounterColumnType => "counter"
      case SchemaTypes.DateType => "date"
      case SchemaTypes.DecimalType => "decimal"
      case SchemaTypes.DoubleType => "double"
      case SchemaTypes.DynamicCompositeType => "dynamiccomposite"
      case SchemaTypes.EmptyType => "empty"
      case SchemaTypes.FloatType => "float"
      case SchemaTypes.InetAddressType => "inetaddress"
      case SchemaTypes.Int32Type => "int"
      case SchemaTypes.IntegerType => "int"
      case SchemaTypes.LexicalUUIDType => "lexicaluuid"
      case SchemaTypes.ListType => "list"
      case SchemaTypes.LocalByPartionerType => "localbypartitioner"
      case SchemaTypes.LongType => "bigint"
      case SchemaTypes.MapType => "map"
      case SchemaTypes.ReversedType => "ordered by DESC "
      case SchemaTypes.SetType => "set"
      case SchemaTypes.TimeUUIDType => "timeuuid"
      case SchemaTypes.TimestampType => "timestamp"
      case SchemaTypes.TypeParser => "typeparser"
      case SchemaTypes.UTF8Type =>  "varchar"
      case SchemaTypes.UUIDType => "uuid"
      case _ => "FIXME"
      //  throw new Nothing(String.format("Could not map supplied '%s' to a known type", sDataType))
    }
  }
}

