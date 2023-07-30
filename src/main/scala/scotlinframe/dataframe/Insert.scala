package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.DataFrame
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*

class InsertClause(
    private[scotlinframe] val ktInsertClause: ktapi.InsertClause[?]
):
  def at(index: Int): DataFrame = DataFrame(
    ktapi.InsertKt.at(ktInsertClause, index)
  )
  def after(columnName: String): DataFrame = DataFrame(
    ktapi.InsertKt.after(ktInsertClause, columnName)
  )
