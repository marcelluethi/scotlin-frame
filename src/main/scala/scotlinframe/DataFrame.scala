package scotlinframe

import org.jetbrains.kotlinx.dataframe.api.{
    DescribeKt => KtDescribeKt,
    ConstructorsKt => KtConstructorsKt, 
    DataColumnTypeKt => KtDataColumnTypeKt, 
    DataFrameGetKt => KtDataFrameGetKt,
    NullsKt => KtNullsKt,
    AddKt => KtAddKt,
    Infer => KtInfer, 
    Split => KtSplit, 
    SplitKt => KtSplitKt, 
    SplitWithTransform => KtSplitWithTransform, 
    FilterKt => KtFilterKt,
    ValuesKt => KtValuesKt,
    PrintKt => KtPrintKt,
    TypeConversionsKt => KtTypeConversionsKt
}

import org.jetbrains.kotlinx.dataframe.{
    DataColumn => KtDataColumn,
    DataColumnKt => KtDataColumnKt,
    DataRow => KtDataRow,  
    DataRowKt => KtDataRowKt,
    DataFrame => KtDataFrame, 
    DataFrameKt => KtDataFrameKt,
}


import scala.jdk.CollectionConverters.*
import scala.util.Try
import java.io.File
import java.net.URL

class DataFrame(private[scotlinframe] val ktDataFrame: KtDataFrame[?]):
  
  def columns(): Seq[DataColumn[?]] = ktDataFrame.columns().asScala.toSeq.map(DataColumn(_))

  def add(column : DataColumn[?]) : DataFrame = DataFrame(KtAddKt.add(ktDataFrame, column.ktDataColumn))

  def add(df : DataFrame) : DataFrame = DataFrame(KtAddKt.add(ktDataFrame, df.ktDataFrame))

  def get[A](columnName : String) : DataColumn[A] = 
    val colAsAny = ktDataFrame.get(columnName) : KtDataColumn[?]
    DataColumn(colAsAny.asInstanceOf[KtDataColumn[A]])

  def get(columnNames : Seq[String]) : DataFrame = 
    if columnNames.isEmpty then DataFrame.empty
    else DataFrame(ktDataFrame.get(columnNames.head, columnNames.tail : _*))

  def apply(i : Int) : DataRow = DataRow(ktDataFrame.get(i))

  def rows() : Iterable[DataRow] = KtDataFrameGetKt.rows(ktDataFrame).asScala.map(DataRow(_))

  def filter(predicate : DataRow => Boolean) : DataFrame = 
    
    // TODO it seems the second argument is not needed in the predictate. Why?
    val ktPred : kotlin.jvm.functions.Function2[KtDataRow[?], KtDataRow[?], java.lang.Boolean] = 
        (row : KtDataRow[?], _ : KtDataRow[?]) =>  predicate(DataRow(row)) 
    val filtered = KtFilterKt.filter(ktDataFrame, ktPred)
    DataFrame(filtered)

  //def split() = MySplit(SplitKt.split(dataFrame, "age"))

  def dropNA(whereAllNA : Boolean) : DataFrame = DataFrame(KtNullsKt.dropNA(ktDataFrame, whereAllNA))
  def dropNA(columns : Seq[String], whereAllNA : Boolean) : DataFrame = DataFrame(KtNullsKt.dropNA(ktDataFrame, columns.toArray, whereAllNA))

  def print(rowsLimit : Int = 10, valuesLimit : Int =  40, borders : Boolean = false, alignLeft : Boolean = false, columnTypes : Boolean = false, title : Boolean = false) : Unit = 
    KtPrintKt.print(ktDataFrame, rowsLimit, valuesLimit, borders, alignLeft, columnTypes, title)

  def toMap() : Map[String, Seq[?]] = 
    val ktMap = KtTypeConversionsKt.toMap(ktDataFrame)
    ktMap.asScala.toMap.map((k, v) => (k, v.asScala.toSeq))

  override def toString() : String = ktDataFrame.toString()

  def describe() : DataFrame = DataFrame(KtDescribeKt.describe(ktDataFrame))

object DataFrame:

    def empty : DataFrame =
        DataFrame(KtConstructorsKt.emptyDataFrame())

    def ofColumns(columns : Seq[DataColumn[?]]) : DataFrame =
        DataFrame(KtConstructorsKt.dataFrameOf(columns.map(_.ktDataColumn).asJava))
        
    def readCSVFromFile(file : File, delimiter : Char = ',') : Try[DataFrame]  = 
      DataFrameIO.readCSVFromFile(file, delimiter)

    def readCSVFromUrl(url : URL, delimiter : Char = ',') : Try[DataFrame]  = 
      DataFrameIO.readCSVFromUrl(url, delimiter)

    def readJsonFromUrl(url : URL) : Try[DataFrame] = 
        DataFrameIO.readJsonFromUrl(url)

    def readJsonFromFile(file : File) : Try[DataFrame] = 
        DataFrameIO.readJsonFromFile(file)