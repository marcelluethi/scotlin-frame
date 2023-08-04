package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.{DataColumn, DataFrame, DataRow}
import org.jetbrains.kotlinx.dataframe.{api => ktapi}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}
import scala.jdk.CollectionConverters.*
import scotlinframe.ColumnAccessor
import scotlinframe.ColumnMapper
import scotlinframe.asColumn
import scotlinframe.Aggregator

class GroupByClause(
    column: ColumnAccessor[?],
    private[scotlinframe] val ktGroupBy: ktapi.GroupBy[?, ?]
):

  def toDataFrame(groupedColumn: ColumnAccessor[?]): DataFrame =
    DataFrame(ktGroupBy.toDataFrame(groupedColumn.name))

  def aggregate(aggregators: Aggregator[?, ?]*): DataFrame =

    val tmpGroup = "tmpGroupInAggregate".asColumn[DataFrame]
    val groupedDf = toDataFrame(tmpGroup)
    val mappedCols = aggregators.map(aggregator => aggregator.columnMapper(groupedDf, tmpGroup))
    // new ColumnMapper[DataFrame, A](tmpGroup, df => aggregator.run(df.get(aggregator.column)), None)

    groupedDf.replace(tmpGroup).withMappedColumns(mappedCols: _*)

    //   val groupedDf = df.groupBy("City".asColumn[String]).toDataFrame(grouped)
    // groupedDf.print()

    // println("===============aggregated================")

    // val sumAge = ColumnMapper(grouped, df => df.get("Age".asColumn[Int]).values.sum).rename("sumAge")

    // val maxAge =
    //   ColumnMapper(grouped, df => df.get("Age".asColumn[Int]).values.max).rename("MaxAge").rename("maxAge")

    // groupedDf.replace(grouped).withMappedColumns(sumAge, maxAge).print()

  // def aggregate[A : ToKType](aggregators : ColumnAggregator[?]*): DataFrame =
  //   val groupedColumnName = "grouped"
  //   toDataFrame(groupedColumnName).
  //      .mapRows(row =>
  //       val col = row.get[DataFrame](groupedColumnName).get[A](columnName)
  //       f(col), "mappedColumn"
  //      ).replace(groupedColumnName)

  // .mapRows(row =>
  //   row.get[DataFrame]("NewCity").get[Int]("Age").values.map(_.nn).min
  //   , "MaxAge"
  // ).replace("NewCity").print()

  // def aggregate[A](aggregateFuns : Map[String, Function1[DataColumn[A | Null], A]]) : DataFrame =
  //   // val newCols = for
  //   //   group <- ktGroupBy.getGroups().toList().asScala
  //   //   columnName <- group.columnNames().asScala
  //   //   if aggregateFuns.contains(columnName)
  //   // yield
  //   //   val col = DataFrame(group).get[A](columnName)
  //   //   aggregateFuns(columnName)(col)
  //   def rowForGroup : DataRow = ???

  //   val rows for group <- ktGroupBy.getGroups.toList().asScala yield
  //     DataFrame(group)
  //   ktGroupBy.

  //   val ktAggFun: kotlin.jvm.functions.Function2[AggregateGroupedDsl[A], AggregateGroupedDsl[A], R] = ???
  //   //   ?
  //   // ], ktdataframe.DataRow[?], java.lang.Boolean] =
  //   //   (row: ktdataframe.DataRow[?], _: ktdataframe.DataRow[?]) => predicate(DataRow(row))
  //   // val filtered = ktapi.FilterKt.filter(ktDataFrame, ktPred)

  //   //ktGroupBy.aggregate(ktAggFun)
  //   //ktapi.GroupByKt.aggregate(ktGroupBy, columnName, aggregateFuns)
  //   ???
