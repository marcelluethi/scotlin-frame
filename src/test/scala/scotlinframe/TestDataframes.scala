package scotlinframe

import scotlinframe.DateTime.LocalDate
import scotlinframe.DateTime.LocalTime
import scotlinframe.DateTime.LocalDateTime

object TestDataframes:

  def simpleTestFrame: DataFrame =
    DataFrame.ofColumns(
      DataColumn.of[String]("Name", Seq("Alice", "Bob", "Charlie")),
      DataColumn.of[Int]("Age", Seq(15, 20, 100))
    )

  def simpleTestFrameWithDuplicateRow: DataFrame =
    DataFrame.ofColumns(
      DataColumn.of[String]("Name", Seq("Alice", "Bob", "Charlie", "Bob")),
      DataColumn.of[Int]("Age", Seq(15, 20, 15, 20))
    )

  def testFrameWithEveryType: DataFrame =
    DataFrame.ofColumns(
      DataColumn.of[String]("string", Seq.empty),
      DataColumn.of[Int]("int", Seq.empty),
      DataColumn.of[Long]("long", Seq.empty),
      DataColumn.of[Float]("float", Seq.empty),
      DataColumn.of[Double]("double", Seq.empty),
      DataColumn.of[Boolean]("boolean", Seq.empty),
      DataColumn.of[LocalTime]("time", Seq.empty),
      DataColumn.of[LocalDate]("data", Seq.empty),
      DataColumn.of[LocalDateTime]("datetime", Seq.empty)
    )
