package scotlinframe

import scala.jdk.CollectionConverters.*
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import scala.util.Try

class DataRow(private[scotlinframe] val ktdataRow: ktdataframe.DataRow[?]):

  def df: DataFrame = DataFrame(ktdataRow.df())

  def get[A](column: String): A | Null = ktdataRow.get(column).asInstanceOf[A]

  def prev: Option[DataRow] =
    ktapi.DataRowApiKt.prev(ktdataRow) match
      case null => None
      case prev => Some(DataRow(prev))

  override def toString(): String = ktdataRow.toString()
