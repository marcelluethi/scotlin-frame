package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.{DataFrame, DataColumn}
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*

class ReplaceClause(
    private[scotlinframe] val ktReplaceClause: ktapi.ReplaceClause[?, ?]
):
  def withColumn(column: DataColumn[?]): DataFrame = DataFrame(
    ktapi.ReplaceKt.`with`(ktReplaceClause, column.ktDataColumn)
  )
