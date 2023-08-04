package scotlinframe

import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}

import scala.jdk.CollectionConverters.*
import org.jetbrains.kotlinx.dataframe.DataFrameKt

import utils.ToKType
import utils.KotlinToScotlinFrameType

class DataColumn[A](
    private[scotlinframe] val ktDataColumn: ktdataframe.DataColumn[A]
):
  override def toString(): String = ktDataColumn.toString()

  def name: String = ktDataColumn.name()

  def rename(newName: String): DataColumn[A] = DataColumn(ktDataColumn.rename(newName))

  def get(index: Int): A = KotlinToScotlinFrameType.fromKotlin[A](ktDataColumn.get(index))
  def values: Seq[A] = ktDataColumn.values().asScala.map(KotlinToScotlinFrameType.fromKotlin[A]).toSeq

  def get(range: Range): DataColumn[A] = get(range.toSeq)

  def get(iterable: Iterable[Int]): DataColumn[A] =
    val it = iterable.map(_.asInstanceOf[java.lang.Integer]).asJava
    DataColumn(ktDataColumn.get(it))

  def distinct(): DataColumn[A] = DataColumn(ktDataColumn.distinct())

  def dropNA(): DataColumn[A] = DataColumn(ktapi.NullsKt.dropNA(ktDataColumn))

  def map[B](f: A => B)(using toKType: ToKType[B]): DataColumn[B] =
    val ktFun: kotlin.jvm.functions.Function1[A, B] = (a: A) => f(KotlinToScotlinFrameType.fromKotlin(a))

    val ktDataColumnB = ktapi.MapKt.map(ktDataColumn, toKType.ktype, ktapi.Infer.None, ktFun)
    DataColumn(ktDataColumnB)

  def split[R1: ToKType, R2: ToKType](
      columnName1: String,
      columnName2: String,
      updateFun: A => (R1, R2)
  ): (DataColumn[R1 | Null], DataColumn[R2 | Null]) =
    values.map(updateFun).unzip match
      case (col1, col2) =>
        (DataColumn.of(columnName1, col1), DataColumn.of(columnName2, col2))

object DataColumn:
  def of[A](name: String, values: Seq[A])(using
      toKType: ToKType[A]
  ): DataColumn[A | Null] =

    val col = ktdataframe.DataColumn.Companion.createValueColumn(
      name,
      values.asJava,
      toKType.ktype,
      ktapi.Infer.None,
      null
    )
    DataColumn[A | Null](col)
