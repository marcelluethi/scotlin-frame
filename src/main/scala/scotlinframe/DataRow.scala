package scotlinframe

import org.jetbrains.kotlinx.dataframe.{api => ktApi}
import org.jetbrains.kotlinx.dataframe.api.{
    ConstructorsKt => KtConstructorsKt, 
    DataRowApiKt => KtDataRowApiKt,
}

import scala.jdk.CollectionConverters.*

import org.jetbrains.kotlinx.dataframe.{DataRowKt => KtDataRowKt}
import org.jetbrains.kotlinx.dataframe.{DataRow => KtDataRow}

class DataRow(private[scotlinframe] val ktdataRow : KtDataRow[?]):
  
  def df : DataFrame = DataFrame(ktdataRow.df())

  def get[A](column : String) : A = ktdataRow.get(column).asInstanceOf[A]

  override def toString(): String = ktdataRow.toString()



