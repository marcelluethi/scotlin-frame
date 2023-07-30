package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.DataFrame
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*

class ConvertClause(dataframe: DataFrame, columnNames: String*):
  def to[A: ToKType](using toKType: ToKType[A]): DataFrame =
    val ktConvertClause =
      ktapi.ConvertKt.convert(dataframe.ktDataFrame, columnNames: _*)
    DataFrame(ktapi.ConvertKt.to(ktConvertClause, toKType.ktype))
