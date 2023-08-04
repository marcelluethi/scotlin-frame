package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.{DataFrame, DataColumn}
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*
import scotlinframe.ColumnAccessor

class MapClauseN[A: ToKType](
    dataframe: DataFrame,
    columns: Seq[DataColumn[A | Null]]
):
  def add(): DataFrame =
    columns.foldLeft(dataframe)((df, col) => df.add(col))

  def insertAfter(columnAccessor: ColumnAccessor[?]): DataFrame =
    columns
      .foldLeft((dataframe, columnAccessor))((dfWithAccessor, col) =>
        (dfWithAccessor._1.insert(col).after(dfWithAccessor._2), dfWithAccessor._2)
      )
      ._1
