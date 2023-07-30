package scotlinframe

import munit.*

class DataFrameSplitMergeTests extends FunSuite:

  test("A column in a dataframe can be split into multiple columns"):
    val df = DataFrame.ofColumns(
      DataColumn.of[String]("From_To", Seq("1-3", "2-4", "3-5"))
    )

    val newDf = df
      .split("From_To")
      .by((a: String) => (a.nn.split("-")(0), a.nn.split("-")(1)))
      .into("From", "To")

    assertEquals(newDf.get[String]("From").values, Seq("1", "2", "3"))
    assertEquals(newDf.get[String]("To").values, Seq("3", "4", "5"))

  test("Two columns can be merged into one"):
    val df = DataFrame.ofColumns(
      DataColumn.of[String]("From", Seq("1", "2", "3")),
      DataColumn.of[String]("To", Seq("3", "4", "5"))
    )

    val newDf =
      df.merge("From", "To")
        .withFunction((a: String, b: String) => s"${a.nn}-${b.nn}")
        .into("From_To")
    assertEquals(
      newDf.get[String]("From_To").values.map(_.nn),
      Seq("1-3", "2-4", "3-5")
    )
