package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.{DataFrame, DataColumn}
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*
import scotlinframe.dataframe.SplitIntoClause

class SplitByClause(
    dataframe: DataFrame,
    columnName: String
):
  def by[A, R1: ToKType, R2: ToKType](
      splitter: A | Null => (R1, R2)
  ): SplitIntoClause[R1, R2] =
    val columnValues = dataframe.get[A](columnName).values.map(splitter)
    SplitIntoClause(dataframe, columnName, columnValues)

class SplitIntoClause[R1: ToKType, R2: ToKType](
    dataframe: DataFrame,
    columnName: String,
    columnValues: Seq[(R1, R2)]
):

  def into(newColumnName1: String, newColumnName2: String): DataFrame =
    val (columnValues1, columnValues2) = columnValues.unzip
    val newColumn1 = DataColumn.of(newColumnName1, columnValues1)
    val newColumn2 = DataColumn.of(newColumnName2, columnValues2)
    dataframe
      .replace(columnName)
      .withColumn(newColumn1)
      .insert(newColumn2)
      .after(newColumn1.name)
