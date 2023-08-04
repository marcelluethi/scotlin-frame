package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.{DataFrame, DataColumn}
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*
import scotlinframe.dataframe.SplitIntoClause
import scotlinframe.ColumnAccessor

class SplitByClause[A](
    dataframe: DataFrame,
    column: ColumnAccessor[A]
):

  def by[R1: ToKType, R2: ToKType](
      splitter: A | Null => (R1, R2)
  ): SplitIntoClause[A, R1, R2] =
    val columnValues = dataframe.get[A](column).values.map(splitter)
    SplitIntoClause(dataframe, column, columnValues)

class SplitIntoClause[A, R1: ToKType, R2: ToKType](
    dataframe: DataFrame,
    column: ColumnAccessor[A],
    columnValues: Seq[(R1, R2)]
):

  def into(newColumn1: ColumnAccessor[R1], newColumn2: ColumnAccessor[R2]): DataFrame =
    val (columnValues1, columnValues2) = columnValues.unzip
    val newCol1 = DataColumn.of(newColumn1.name, columnValues1)
    val newCol2 = DataColumn.of(newColumn2.name, columnValues2)
    dataframe
      .replace(column)
      .withColumn(newCol1)
      .insert(newCol2)
      .after(newColumn1)
