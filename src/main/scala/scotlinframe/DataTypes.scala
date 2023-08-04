package scotlinframe

import kotlin.reflect.KType
import scotlinframe.utils.ToKType
import scotlinframe.DateTime.LocalDateTime
import scotlinframe.DateTime.LocalDate
import scotlinframe.DateTime.LocalTime

enum DataType:
  case Int
  case Short
  case Double
  case Float
  case Boolean
  case Long
  case String
  case LocalDateTime
  case LocalDate
  case LocalTime
  case UnknownType

object DataType:
  def FromKType(ktype: KType): DataType =
    ktype match
      case ktype if ktype == summon[ToKType[Int]].ktype     => DataType.Int
      case ktype if ktype == summon[ToKType[Short]].ktype   => DataType.Short
      case ktype if ktype == summon[ToKType[Double]].ktype  => DataType.Double
      case ktype if ktype == summon[ToKType[Float]].ktype   => DataType.Float
      case ktype if ktype == summon[ToKType[Boolean]].ktype => DataType.Boolean
      case ktype if ktype == summon[ToKType[Long]].ktype    => DataType.Long
      case ktype if ktype == summon[ToKType[String]].ktype  => DataType.String
      case ktype if ktype == summon[ToKType[LocalDateTime]].ktype =>
        DataType.LocalDateTime
      case ktype if ktype == summon[ToKType[LocalDate]].ktype =>
        DataType.LocalDate
      case ktype if ktype == summon[ToKType[LocalTime]].ktype =>
        DataType.LocalTime
      // TODO add a dataype for a dataframe, in case we have nested columns
      case _ => DataType.UnknownType
