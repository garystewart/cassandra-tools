package ganda


/**
 * Created by M06F525 on 28/04/2015.
 */

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
      case SchemaTypes.BooleanType =>
        return "boolean"
      case SchemaTypes.BytesType =>
        return "blob"
      case SchemaTypes.CollectionType =>
        return "collection"
      case SchemaTypes.ColumnToCollectionType =>
        return "columntocollection"
      case SchemaTypes.CompositeType =>
        return "composite"
      case SchemaTypes.CounterColumnType =>
        return "counter"
      case SchemaTypes.DateType =>
        return "date"
      case SchemaTypes.DecimalType =>
        return "decimal"
      case SchemaTypes.DoubleType =>
        return "double"
      case SchemaTypes.DynamicCompositeType =>
        return "dynamiccomposite"
      case SchemaTypes.EmptyType =>
        return "empty"
      case SchemaTypes.FloatType =>
        return "float"
      case SchemaTypes.InetAddressType =>
        return "inetaddress"
      case SchemaTypes.Int32Type =>
        return "int"
      case SchemaTypes.IntegerType =>
        return "int"
      case SchemaTypes.LexicalUUIDType =>
        return "lexicaluuid"
      case SchemaTypes.ListType =>
        return "list"
      case SchemaTypes.LocalByPartionerType =>
        return "localbypartitioner"
      case SchemaTypes.LongType =>
        return "bigint"
      case SchemaTypes.MapType =>
        return "map"
      case SchemaTypes.ReversedType =>
        return "ordered by DESC "
      case SchemaTypes.SetType =>
        return "set"
      case SchemaTypes.TimeUUIDType =>
        return "timeuuid"
      case SchemaTypes.TimestampType =>
        return "timestamp"
      case SchemaTypes.TypeParser =>
        return "typeparser"
      case SchemaTypes.UTF8Type => return "varchar"
      case SchemaTypes.UUIDType =>
        return "uuid"
      case _ => "FIXME"
      //  throw new Nothing(String.format("Could not map supplied '%s' to a known type", sDataType))
    }
  }
}

