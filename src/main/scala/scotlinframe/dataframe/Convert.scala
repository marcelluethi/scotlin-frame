package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.DataFrame
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*
import scotlinframe.ColumnAccessor

class ConvertClause(dataframe: DataFrame, columns: ColumnAccessor[?]*):
  def to[A: ToKType](using toKType: ToKType[A]): DataFrame =
    val ktConvertClause =
      ktapi.ConvertKt.convert(dataframe.ktDataFrame, columns.map(_.name): _*)
    DataFrame(ktapi.ConvertKt.to(ktConvertClause, toKType.ktype))
