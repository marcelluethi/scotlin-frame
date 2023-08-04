package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.{DataFrame, DataColumn, ColumnAccessor}
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*

class MergeWithClause[A, B](
    dataframe: DataFrame,
    column1: ColumnAccessor[A],
    column2: ColumnAccessor[B]
):

  def withFunction[R: ToKType](merger: (A, B) => R) =
    val colValues =
      dataframe.rows.map(row => merger(row.get[A](column1), row.get[B](column2)))

    MergeIntoClause(dataframe, column1, column2, colValues.toSeq)

class MergeIntoClause[A, B, R: ToKType](
    dataframe: DataFrame,
    column1: ColumnAccessor[A],
    column2: ColumnAccessor[B],
    colValues: Seq[R]
):

  def into(newColumn: ColumnAccessor[R]): DataFrame =
    val newCol = DataColumn.of[R](newColumn.name, colValues.toSeq)
    dataframe.remove(column1).replace(column2).withColumn(newCol)
