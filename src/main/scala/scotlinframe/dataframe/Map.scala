package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.{DataFrame, DataColumn}
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*

class MapClauseN[A: ToKType](
    dataframe: DataFrame,
    columns: Seq[DataColumn[A | Null]]
):
  def add(): DataFrame =
    columns.foldLeft(dataframe)((df, col) => df.add(col))

  def insertAfter(columnName: String): DataFrame =
    columns
      .foldLeft((dataframe, columnName))((dfAndName, col) =>
        (dfAndName._1.insert(col).after(dfAndName._2), dfAndName._2)
      )
      ._1
