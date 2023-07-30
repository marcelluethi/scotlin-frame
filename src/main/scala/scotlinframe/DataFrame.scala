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

class DataFrame(
    private[scotlinframe] val ktDataFrame: ktdataframe.DataFrame[?]
):

  def columns: Seq[DataColumn[?]] =
    ktDataFrame.columns().asScala.toSeq.map(DataColumn(_))

  def columnNames: Seq[String] =
    ktDataFrame.columnNames().asScala.toSeq

  def columnTypes: Seq[DataType] =
    ktDataFrame
      .columnTypes()
      .asScala
      .map(ktype => DataType.FromKType(ktype))
      .toSeq

  def add(column: DataColumn[?]): DataFrame = DataFrame(
    ktapi.AddKt.add(ktDataFrame, column.ktDataColumn)
  )

  def add(df: DataFrame): DataFrame = DataFrame(
    ktapi.AddKt.add(ktDataFrame, df.ktDataFrame)
  )

  def append(row: DataRow): DataFrame =
    DataFrame(ktapi.AppendKt.append(ktDataFrame, row.ktdataRow))

  def remove(columnName: String): DataFrame = DataFrame(
    ktapi.RemoveKt.remove(ktDataFrame, columnName)
  )

  def concat(df: DataFrame): DataFrame = DataFrame(
    ktapi.ConcatKt.concat(ktDataFrame, df.ktDataFrame)
  )

  def parse(parserOptions: ParserOptions) =
    val parserOptionsKt = ktapi.ParserOptions(
      parserOptions.locale,
      parserOptions.dateTimeFormatter,
      parserOptions.dateTimePattern,
      parserOptions.nullStrings.asJava
    )
    DataFrame(ktapi.ParseKt.parse(ktDataFrame, parserOptionsKt))

  def split(columnName: String): SplitByClause =
    SplitByClause(this, columnName)

  def merge(
      column1Name: String,
      column2Name: String
  ): MergeWithClause = MergeWithClause(this, column1Name, column2Name)

  def move(columnNames: String*): MoveClause =
    MoveClause(ktapi.MoveKt.move(ktDataFrame, columnNames: _*))

  def update(columnNames: String*): UpdateClause =
    val updateKt = ktapi.UpdateKt.update(ktDataFrame, columnNames: _*)
    UpdateClause(updateKt)

  def fillNA(columnNames: String*): UpdateClause =
    val updateKt = ktapi.NullsKt.fillNA(ktDataFrame, columnNames: _*)
    UpdateClause(updateKt)

  def convert(columnNames: String*): ConvertClause =
    ConvertClause(this, columnNames: _*)

  def get[A](columnName: String): DataColumn[A | Null] =
    val colAsAny = ktDataFrame.get(columnName): ktdataframe.DataColumn[?]
    DataColumn(colAsAny.asInstanceOf[ktdataframe.DataColumn[A | Null]])

  def get(columnNames: String*): DataFrame =
    if columnNames.isEmpty then DataFrame.empty
    else DataFrame(ktDataFrame.get(columnNames.head, columnNames.tail: _*))

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

  def replace[A](column: String): ReplaceClause =
    ReplaceClause(ktapi.ReplaceKt.replace(ktDataFrame, column))

  def mapRows[A: ToKType](
      f: DataRow => A,
      newColumnName: String
  ): MapClause1[A] =
    val newColumn = DataColumn.of[A](newColumnName, rows.map(f).toSeq)
    MapClause1(this, newColumn)

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
