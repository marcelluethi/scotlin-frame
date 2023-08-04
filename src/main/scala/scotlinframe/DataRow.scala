package scotlinframe

import scala.jdk.CollectionConverters.*
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import scala.util.Try
import utils.KotlinToScotlinFrameType

class DataRow(private[scotlinframe] val ktdataRow: ktdataframe.DataRow[?]):

  def df: DataFrame = DataFrame(ktdataRow.df())

  def get[A](column: ColumnAccessor[A]): A = KotlinToScotlinFrameType.fromKotlin[A](ktdataRow.get(column.name))

  def prev: Option[DataRow] =
    ktapi.DataRowApiKt.prev(ktdataRow) match
      case null => None
      case prev => Some(DataRow(prev))

  override def toString(): String = ktdataRow.toString()
