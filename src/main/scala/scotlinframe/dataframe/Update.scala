package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.{DataFrame, DataColumn, DataRow}
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*
import scotlinframe.dataframe.SplitIntoClause
import scotlinframe.utils.KotlinToScotlinFrameType
import scotlinframe.ColumnAccessor

class UpdateClause(
    private[scotlinframe] val ktUpdateObj: ktapi.Update[?, AnyRef]
):

  def mapRow[R](updateFun: DataRow => R): DataFrame =
    val updateFunKt: kotlin.jvm.functions.Function2[ktapi.AddDataRow[?], Object, R] =
      (
          (r, a) => updateFun(DataRow(r))
      )
    DataFrame(ktapi.UpdateKt.`with`(ktUpdateObj, updateFunKt))

  def mapValue[A, R](updateFun: A => R): DataFrame =
    val updateFunKt: kotlin.jvm.functions.Function2[ktapi.AddDataRow[?], Object, R] =
      (r, a) => updateFun(KotlinToScotlinFrameType.fromKotlin[A](a))
    DataFrame(ktapi.UpdateKt.`with`(ktUpdateObj, updateFunKt))

class MapClause1[A: ToKType](
    dataframe: DataFrame,
    mappedColumn: DataColumn[A | Null]
):
  def add(): DataFrame =
    dataframe.add(mappedColumn)

  def insertAfter[A](column: ColumnAccessor[A]): DataFrame =
    dataframe.insert(mappedColumn).after(column)

  def replace[A](column: ColumnAccessor[A]): DataFrame =
    dataframe.replace(column).withColumn(mappedColumn)
