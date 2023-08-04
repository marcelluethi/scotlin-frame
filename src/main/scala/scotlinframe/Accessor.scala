package scotlinframe

import scala.meta.internal.javacp.BaseType.D
import scotlinframe.utils.ToKType

/** The class holds the name of a column together with its type
  */
case class ColumnAccessor[A](val name: String)

/** Make it easy to create a columnaccessor from a string
  */
extension (columnName: String) def asColumn[A] = ColumnAccessor[A](columnName)

/** A columnmapper takes a dataframe, accesses its column and returns a new column, possibly of a different type and
  * with different name
  */
class ColumnMapper[A, B: ToKType](val column: ColumnAccessor[A], f: A => B, newColumnName: Option[String] = None):

  def map[C: ToKType](g: B => C): ColumnMapper[A, C] = new ColumnMapper[A, C](
    column,
    g.compose(f),
    newColumnName
  )

  def rename(newName: String) = new ColumnMapper[A, B](
    column,
    f,
    Some(newName)
  )

  def run(df: DataFrame): DataColumn[B] =
    val newCol = df.get(column).map(f)
    newColumnName match
      case Some(name) => newCol.rename(name)
      case None       => newCol

object ColumnMapper:
  def id[A: ToKType](accessor: ColumnAccessor[A]) = new ColumnMapper[A, A](accessor, a => a)
  def apply[A, B: ToKType](accessor: ColumnAccessor[A], f: A => B): ColumnMapper[A, B] =
    new ColumnMapper[A, B](accessor, f)

/** The aggregator class is used to aggregate values within a data column. It is intended to be used to aggregate or
  * reduce columns within a nested dataframe,
  *
  * @param column
  *   the column to aggregate
  * @param aggregateFun
  *   the function to apply to the column in the nested dataframe
  */
class Aggregator[A, B: ToKType](
    val column: ColumnAccessor[A],
    val aggregateFun: DataColumn[A] => B,
    val newColumnName: Option[String] = None
):

  def rename(newName: String) = new Aggregator[A, B](
    column,
    aggregateFun,
    Some(newName)
  )

  def columnMapper(df: DataFrame, accessor: ColumnAccessor[DataFrame]): ColumnMapper[DataFrame, B] =
    new ColumnMapper[DataFrame, B](accessor, df => aggregateFun(df.get(column)), newColumnName)

object Aggregator:
  def sum[A: ToKType: Numeric](column: ColumnAccessor[A]) = new Aggregator[A, A](column, col => col.values.sum)
  def max[A: ToKType: Ordering](column: ColumnAccessor[A]) = new Aggregator[A, A](column, col => col.values.max)
  def min[A: ToKType: Ordering](column: ColumnAccessor[A]) = new Aggregator[A, A](column, col => col.values.min)
  def count[A: ToKType](column: ColumnAccessor[A]) = new Aggregator[A, Int](column, col => col.values.size)
  def avg[A: ToKType: Numeric](column: ColumnAccessor[A]) =
    new Aggregator[A, Double](column, col => summon[Numeric[A]].toDouble(col.values.sum) / col.values.size)

  // val sumAge = ColumnMapper(grouped, df => df.get("Age".asColumn[Int]).values.sum).rename("sumAge")
