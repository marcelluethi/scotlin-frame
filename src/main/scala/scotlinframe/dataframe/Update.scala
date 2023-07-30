package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.{DataFrame, DataColumn, DataRow}
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*
import scotlinframe.dataframe.SplitIntoClause

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
      (r, a) => updateFun(a.asInstanceOf[A])
    DataFrame(ktapi.UpdateKt.`with`(ktUpdateObj, updateFunKt))

class MapClause1[A: ToKType](
    dataframe: DataFrame,
    column: DataColumn[A | Null]
):
  def add(): DataFrame =
    dataframe.add(column)

  def insertAfter(columnName: String): DataFrame =
    dataframe.insert(column).after(columnName)

  def replace(columnName: String): DataFrame =
    dataframe.replace(columnName).withColumn(column)
