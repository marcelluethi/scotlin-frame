package scotlinframe

import munit.*
import java.io.File
import scala.util.Failure
import scala.util.Success

import scotlinframe.DateTime.LocalDate

class DataFrameCreationTests extends FunSuite:

  test("can create a dataframe from scratch"):

    val name = "Name".asColumn[String]
    val age = "Age".asColumn[Int]

    val df = DataFrame.ofColumns(
      DataColumn.of[String]("Name", Seq("Alice", "Bob", "Charlie")),
      DataColumn.of[Int]("Age", Seq(15, 20, 100))
    )

    assertEquals(df.get(name).values, Seq("Alice", "Bob", "Charlie"))
    assertEquals(df.get(age).values, Seq(15, 20, 100))
