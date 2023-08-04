package scotlinframe

import org.jetbrains.kotlinx
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}

import scala.jdk.CollectionConverters.*
import scala.util.Try
import java.io.File
import java.net.URL
import scala.meta.internal.javacp.BaseType.I
import org.jetbrains.kotlinx.dataframe.ColumnsContainer

import utils.ToKType
import kotlin.ranges.IntRange
import scotlinframe.dataframe.*
import org.jetbrains.kotlinx.dataframe.DataFrameKt
import org.jetbrains.kotlinx.dataframe.jupyter.CellRenderer
import org.jetbrains.kotlinx.dataframe.io.DataFrameHtmlData
import scotlinframe.utils.KotlinToScotlinFrameType
import scotlinframe.ColumnMapper
import scotlinframe.ColumnAccessor

/** A DataFrame is a collection of columns of data.
  */
class DataFrame(
    private[scotlinframe] val ktDataFrame: ktdataframe.DataFrame[?]
):

  /** Returns the columns of the DataFrame.
    */
  def columns: Seq[DataColumn[?]] =
    ktDataFrame.columns().asScala.toSeq.map(DataColumn(_))

  /** Returns the column names of the DataFrame.
    */
  def columnNames: Seq[String] =
    ktDataFrame.columnNames().asScala.toSeq

  /** Returns the types of the columns of the DataFrame.
    */
  def columnTypes: Seq[DataType] =
    ktDataFrame
      .columnTypes()
      .asScala
      .map(ktype => DataType.FromKType(ktype))
      .toSeq

  /** Returns the specified column
    */
  def get[A](column: ColumnAccessor[A]): DataColumn[A] =
    val colAsAny = ktDataFrame.get(column.name): ktdataframe.DataColumn[?]
    DataColumn[A](colAsAny.asInstanceOf[ktdataframe.DataColumn[A]])

  /** Returns a dataframe with the selected columns
    */
  def get(column1: ColumnAccessor[?], columns: ColumnAccessor[?]*): DataFrame =
    DataFrame(ktDataFrame.get(column1.name, columns.map(_.name): _*))

  /** Adds a new column to the end of the dataframe.
    */
  def add(column: DataColumn[?]): DataFrame = DataFrame(
    ktapi.AddKt.add(ktDataFrame, column.ktDataColumn)
  )

  /** Adds the column of the provided dataframe to the end of the dataframe.
    */
  def add(otherDf: DataFrame): DataFrame = DataFrame(
    ktapi.AddKt.add(ktDataFrame, otherDf.ktDataFrame)
  )

  /** Adds new columns by mapping values of existing columns
    */
  def add(columnMappers: ColumnMapper[?, ?]*): DataFrame =
    columnMappers.foldLeft(this)((df, mapper) => df.add(mapper.run(df)))

  /** Appends a row to the end of the dataframe.
    */
  def append(row: DataRow): DataFrame =
    DataFrame(ktapi.AppendKt.append(ktDataFrame, row.ktdataRow))

  /** Removes the specified column from the dataframe.
    */
  def remove(column: ColumnAccessor[?]): DataFrame = DataFrame(
    ktapi.RemoveKt.remove(ktDataFrame, column.name)
  )

  /** Concatenates the rows of this dataframe with the rows of the provided dataframe.
    */
  def concat(otherDf: DataFrame): DataFrame = DataFrame(
    ktapi.ConcatKt.concat(ktDataFrame, otherDf.ktDataFrame)
  )

  /** Returns a new dataframe in which strings are parsed and if possible automatically converted to an appropriate type
    */
  def parse(parserOptions: ParserOptions) =
    val parserOptionsKt = ktapi.ParserOptions(
      parserOptions.locale,
      parserOptions.dateTimeFormatter,
      parserOptions.dateTimePattern,
      parserOptions.nullStrings.asJava
    )
    DataFrame(ktapi.ParseKt.parse(ktDataFrame, parserOptionsKt))

  /** Splits a column into multiple columns
    */
  def split[A](column: ColumnAccessor[A]): SplitByClause[A] =
    SplitByClause(this, column)

  /** Merges two columns
    */
  def merge[A, B](
      column1: ColumnAccessor[A],
      column2: ColumnAccessor[B]
  ): MergeWithClause[A, B] = MergeWithClause[A, B](this, column1, column2)

  /** Move a column within the dataframe
    */
  def move(columns: ColumnAccessor[?]*): MoveClause =
    val columnNames = columns.map(_.name)
    MoveClause(ktapi.MoveKt.move(ktDataFrame, columnNames: _*))

  def update(columns: ColumnAccessor[?]*): UpdateClause =
    val updateKt = ktapi.UpdateKt.update(ktDataFrame, columns.map(_.name): _*)
    UpdateClause(updateKt)

  def fillNA(columns: ColumnAccessor[?]*): UpdateClause =
    val updateKt = ktapi.NullsKt.fillNA(ktDataFrame, columns.map(_.name): _*)
    UpdateClause(updateKt)

  def convert(columns: ColumnAccessor[?]*): ConvertClause =
    ConvertClause(this, columns: _*)

  def head(n: Int): DataFrame = DataFrame(
    ktapi.HeadKt.head(ktDataFrame, n)
  )

  def take(n: Int): DataFrame = DataFrame(
    ktapi.TakeKt.take(ktDataFrame, n)
  )
  def drop(n: Int): DataFrame = DataFrame(
    ktapi.DropKt.drop(ktDataFrame, n)
  )

  def apply(i: Int): DataRow = DataRow(ktDataFrame.get(i))

  def apply(range: Range): DataFrame =
    val ktRange = IntRange(range.start, range.end - 1)
    DataFrame(ktapi.DataFrameGetKt.getRows(ktDataFrame, ktRange))

  def apply(indices: Iterable[Int]): DataFrame =
    DataFrame(
      ktapi.DataFrameGetKt.getRows(
        ktDataFrame,
        indices.map(i => i.asInstanceOf[java.lang.Integer]).asJava
      )
    )

  def rows: Iterable[DataRow] =
    ktapi.DataFrameGetKt.rows(ktDataFrame).asScala.map(DataRow(_))

  def filter(predicate: DataRow => Boolean): DataFrame =

    // TODO it seems the second argument is not needed in the predictate. Why?
    val ktPred: kotlin.jvm.functions.Function2[ktdataframe.DataRow[
      ?
    ], ktdataframe.DataRow[?], java.lang.Boolean] =
      (row: ktdataframe.DataRow[?], _: ktdataframe.DataRow[?]) => predicate(DataRow(row))
    val filtered = ktapi.FilterKt.filter(ktDataFrame, ktPred)
    DataFrame(filtered)

  def map[A](f: DataRow => A): Seq[A] =
    rows.map(f).toSeq

  /** It applies the provided column mappers to the data frame and returns a new data frame with the mapped columns.
    */
  // def mapColumns(columnMappers : ColumnMapper[?, ?]*) : DataFrame =

  //   // it might happen that several mappers map the same column.
  //   // In this case we must not replace the column until all the mappers
  //   // have been applied. Therefore, we start to build a map that add for each
  //   // column the mapped columns
  //   val columnMap = columnMappers.map(mapper => mapper.column -> mapper.run(this) )
  //     .groupBy(_._1)
  //     .view
  //     .mapValues(_.map(_._2))
  //     .toMap

  //   // now we can replace the columns
  //   columnMap.keys.foldLeft(this) { (df, column) =>
  //     val mappedColumns = columnMap(column)
  //     df.replace(column).withColumns(mappedColumns)
  //   }

  // def mapToColumn[A](columnName: String, f: DataRow => A)(using
  //     toKType: ToKType[A]
  // ): DataColumn[A] =
  //   val ktFun: kotlin.jvm.functions.Function2[ktdataframe.DataRow[
  //     ?
  //   ], ktdataframe.DataRow[?], A] =
  //     (row: ktdataframe.DataRow[?], _: ktdataframe.DataRow[?]) =>
  //       f(DataRow(row))

  //   val ktype = toKType.ktype
  //   DataColumn(
  //     ktapi.MapKt
  //       .mapToColumn(ktDataFrame, columnName, ktype, ktapi.Infer.Nulls, ktFun)
  //   )

  def insert[A](column: DataColumn[A]): InsertClause =
    InsertClause(ktapi.InsertKt.insert(ktDataFrame, column.ktDataColumn))

  def insert(columnMapper: ColumnMapper[?, ?]): InsertClause =
    this.insert(columnMapper.run(this))

  def replace(column: ColumnAccessor[?]): ReplaceClause =
    ReplaceClause(this, ktapi.ReplaceKt.replace(ktDataFrame, column.name))

  def mapRows[A: ToKType](
      f: DataRow => A,
      newColumn: ColumnAccessor[?]
  ): MapClause1[A] =
    val newDataColumn = DataColumn.of[A](newColumn.name, rows.map(f).toSeq)
    MapClause1(this, newDataColumn)

  def mapRows[A: ToKType](f: DataRow => Map[String, A]): MapClauseN[A] =

    val columnValueMap = scala.collection.mutable.Map[String, Seq[A]]()
    for row <- rows do
      val mappedRow = f(row)
      for (columnName, value) <- mappedRow do
        val values = columnValueMap.getOrElseUpdate(columnName, Seq())
        columnValueMap.update(columnName, values :+ value)

    val columns = for columnName <- columnValueMap.keys yield
      val values = columnValueMap(columnName)
      require(
        values.size == rowsCount,
        s"Column $columnName has ${values.size} values, but the dataframe has $rowsCount rows"
      )
      DataColumn.of[A](columnName, values.toSeq)

    MapClauseN(this, columns.toSeq)

  def dropNA(whereAllNA: Boolean): DataFrame = DataFrame(
    ktapi.NullsKt.dropNA(ktDataFrame, whereAllNA)
  )
  def dropNA(whereAllNA: Boolean, columnNames: String*): DataFrame = DataFrame(
    ktapi.NullsKt.dropNA(ktDataFrame, columnNames.toArray, whereAllNA)
  )

  def maxBy(columnName: String): DataRow =
    DataRow(ktapi.MaxKt.maxBy(ktDataFrame, columnName))

  def min(columnName: String): Any =
    ktapi.MinKt.min(ktDataFrame, columnName)

  def minBy(columnName: String): DataRow =
    DataRow(ktapi.MinKt.minBy(ktDataFrame, columnName))

  def minOf[R](f: DataRow => R)(using ord: Ordering[R]): R =
    // using the kotlin minOf function is difficult,
    // as the types are very complicated. Using my own implementation
    val minRow = rows.minBy(f)
    f(minRow)

  def maxOf[R](f: DataRow => R)(using ord: Ordering[R]): R =
    // using the kotlin maxOf function is difficult,
    // as the types are very complicated. Using my own implementation
    val maxRow = rows.maxBy(f)
    f(maxRow)

  def sortBy(columnNames: String*): DataFrame =
    DataFrame(ktapi.SortKt.sortBy(ktDataFrame, columnNames: _*))

  def sortByDescending(columnNames: String*): DataFrame =
    DataFrame(ktapi.SortKt.sortByDesc(ktDataFrame, columnNames: _*))

  def groupBy[A](column: ColumnAccessor[A]): GroupByClause =
    GroupByClause(column, ktapi.GroupByKt.groupBy(ktDataFrame, column.name))

  def shuffle(): DataFrame =
    DataFrame(ktapi.ShuffleKt.shuffle(ktDataFrame))

  def reverse: DataFrame =
    DataFrame(ktapi.ReverseKt.reverse(ktDataFrame))

  def rowsCount: Int =
    ktDataFrame.rowsCount()

  def columnsCount: Int =
    ktDataFrame.columnsCount()

  def count: Int = ktapi.CountKt.count(ktDataFrame)

  def countDistinct: Int =
    ktapi.CountDistinctKt.countDistinct(ktDataFrame)

  def countDistinct(column1: String, columns: String*): Int =
    ktapi.CountDistinctKt.countDistinct(ktDataFrame, (column1 +: columns): _*)

  def sum: DataRow =
    DataRow(ktapi.SumKt.sum(ktDataFrame))

  def median: DataRow =
    DataRow(ktapi.MedianKt.median(ktDataFrame))

  def mean(skipNA: Boolean): DataRow =
    DataRow(ktapi.MeanKt.mean(ktDataFrame, skipNA))

  def std(skipNA: Boolean, ddof: Int = 1): DataRow =
    DataRow(ktapi.StdKt.std(ktDataFrame, skipNA, ddof))

  def print(
      rowsLimit: Int = 10,
      valuesLimit: Int = 40,
      borders: Boolean = false,
      alignLeft: Boolean = false,
      columnTypes: Boolean = false,
      title: Boolean = false
  ): Unit =
    ktapi.PrintKt.print(
      ktDataFrame,
      rowsLimit,
      valuesLimit,
      borders,
      alignLeft,
      columnTypes,
      title
    )

  def toHTML(rowsLimit: Int = count): HTML =
    import org.jetbrains.kotlinx.dataframe.jupyter.DefaultCellRenderer
    val displayConf = ktdataframe.io.DisplayConfiguration.Companion.getDEFAULT()
    displayConf.setRowsLimit(rowsLimit)

    val htmlData = ktdataframe.io.HtmlKt.toStandaloneHTML(
      ktDataFrame,
      displayConf,
      DefaultCellRenderer.INSTANCE,
      _ => ""
    )
    HTML(htmlData)

  def toMap: Map[String, Seq[?]] =
    val ktMap = ktapi.TypeConversionsKt.toMap(ktDataFrame)
    ktMap.asScala.toMap.map((k, v) => (k, v.asScala.toSeq))

  override def toString(): String = ktDataFrame.toString()

  def describe: DataFrame = DataFrame(ktapi.DescribeKt.describe(ktDataFrame))

object DataFrame:

  def empty: DataFrame =
    DataFrame(ktapi.ConstructorsKt.emptyDataFrame())

  def ofColumns(columns: DataColumn[?]*): DataFrame =
    DataFrame(
      ktapi.ConstructorsKt.dataFrameOf(columns.map(_.ktDataColumn).asJava)
    )

  def readCSVFromFile(file: File, delimiter: Char = ','): Try[DataFrame] =
    DataFrameIO.readCSVFromFile(file, delimiter)

  def readCSVFromUrl(url: URL, delimiter: Char = ','): Try[DataFrame] =
    DataFrameIO.readCSVFromUrl(url, delimiter)

  def readJsonFromUrl(url: URL): Try[DataFrame] =
    DataFrameIO.readJsonFromUrl(url)

  def readJsonFromFile(file: File): Try[DataFrame] =
    DataFrameIO.readJsonFromFile(file)
