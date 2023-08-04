package scotlinframe.examples

import java.net.URL
import scala.util.matching.Regex
import scotlinframe.*
import org.checkerframework.checker.units.qual.A
import scotlinframe.*

@main
def aggregationExample(): Unit =

// create columns
  val df = DataFrame.ofColumns(
    DataColumn.of(
      "Name",
      Seq(
        "Alice Cooper",
        "Bob Dylan",
        "Charlie Daniels",
        "Charlie Chaplin",
        "Bob Marley",
        "Alice Wolf",
        "Charlie Byrd"
      )
    ),
    DataColumn.of(
      "Age",
      Seq(15, 45, 20, 40, 30, 20, 30)
    ),
    DataColumn.of(
      "City",
      Seq(
        "London",
        "Dubai",
        "Moscow",
        "Milan",
        "Tokyo",
        null,
        "Moscow"
      )
    ),
    DataColumn.of(
      "Weight",
      Seq(
        54, 87, null, null, 68, 55, 90
      )
    ),
    DataColumn.of(
      "IsHappy",
      Seq(
        true, true, false, true, true, false, true
      )
    )
  )
  df.print()

  println("==============GroupBy=================")

  val grouped = "Grouped".asColumn[DataFrame]

  val groupedDf = df.groupBy("City".asColumn[String]).toDataFrame(grouped)
  groupedDf.print()

  // println("===============aggregated================")

  df.groupBy("City".asColumn[String])
    .aggregate(
      Aggregator.sum("Age".asColumn[Int]).rename("SumAge"),
      Aggregator.max("Age".asColumn[Int]).rename("MaxAge")
    )
    .print()
