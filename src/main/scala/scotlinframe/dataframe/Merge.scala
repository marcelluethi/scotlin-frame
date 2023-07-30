package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.{DataFrame, DataColumn}
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*

class MergeWithClause(
    dataframe: DataFrame,
    column1Name: String,
    column2Name: String
):
  def withFunction[A, B, R: ToKType](merger: (A | Null, B | Null) => R) =
    val colValues =
      dataframe.rows.map(row => merger(row.get[A](column1Name), row.get[B](column2Name)))

    MergeIntoClause(dataframe, column1Name, column2Name, colValues.toSeq)

class MergeIntoClause[R: ToKType](
    dataframe: DataFrame,
    column1Name: String,
    column2Name: String,
    colValues: Seq[R]
):

  def into(newColumnName: String): DataFrame =
    val newColumn = DataColumn.of[R](newColumnName, colValues.toSeq)
    dataframe.remove(column2Name).replace(column1Name).withColumn(newColumn)
