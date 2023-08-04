package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.{DataFrame, DataColumn}
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*
import scotlinframe.asColumn
import scotlinframe.ColumnMapper

class ReplaceClause(
    dataframe: DataFrame,
    private[scotlinframe] val ktReplaceClause: ktapi.ReplaceClause[?, ?]
):
  /** Replaces the column with the new column
    *
    * @param column
    * @return
    */
  def withColumn(column: DataColumn[?]): DataFrame = DataFrame(
    ktapi.ReplaceKt.`with`(ktReplaceClause, column.ktDataColumn)
  )

  /** Replaces the column with several new columns
    */
  def withColumns(columns: Seq[DataColumn[?]]): DataFrame =

    val df = withColumn(columns.head)
    columns.tail
      .foldLeft((df, columns.head))((drWithLastColumn, colToInsert) =>
        val lastColumnAcc = drWithLastColumn._2.name.asColumn
        (drWithLastColumn._1.insert(colToInsert).after(lastColumnAcc), colToInsert)
      )
      ._1

  def withMappedColumns(columnMappers: ColumnMapper[?, ?]*): DataFrame =

    // it might happen that several mappers map the same column.
    // In this case we must not replace the column until all the mappers
    // have been applied. Therefore, we start to build a map that add for each
    // column the mapped columns
    val columnMap = columnMappers
      .map(mapper => mapper.column -> mapper.run(dataframe))
      .groupBy(_._1)
      .view
      .mapValues(_.map(_._2))
      .toMap

    // now we can replace the columns
    columnMap.keys.foldLeft(dataframe) { (df, column) =>
      val mappedColumns = columnMap(column)
      df.replace(column).withColumns(mappedColumns)
    }
