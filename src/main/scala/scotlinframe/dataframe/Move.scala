package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.DataFrame
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*

class MoveClause(
    private[scotlinframe] val ktMoveClause: ktapi.MoveClause[?, ?]
):
  def to(index: Int): DataFrame = DataFrame(
    ktapi.MoveKt.to(ktMoveClause, index)
  )
  def after(columnName: String): DataFrame = DataFrame(
    ktapi.MoveKt.after(ktMoveClause, columnName)
  )
