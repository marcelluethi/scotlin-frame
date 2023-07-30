package scotlinframe

import munit.*

class DataFrameAccessTests extends FunSuite:

  test("A column can be accessed by name"):
    val extractedColumn = TestDataframes.simpleTestFrame.get[Int]("Age")
    val expectedValues = DataColumn.of[Int]("Age", Seq(15, 20, 100))

    assertEquals(extractedColumn.values, expectedValues.values)

  test("A row can be accessed by index"):
    val extractedRow = TestDataframes.simpleTestFrame(1)
    val extractedValues =
      Seq(extractedRow.get[String]("Name"), extractedRow.get[Int]("Age"))
    val expectedValues = Seq("Bob", 20)

    assertEquals(extractedValues, expectedValues)

  test("Rows can be iterated over using map"):
    val extractedRows =
      TestDataframes.simpleTestFrame.map(row => row.get[String]("Name"))
    val expectedValues = Seq("Alice", "Bob", "Charlie")

    assertEquals(extractedRows, expectedValues)
