package scotlinframe

import munit.*

class DataFrameAccessTests extends FunSuite:

  val name = "Name".asColumn[String]
  val age = "Age".asColumn[Int | Null]

  test("A column can be accessed by name"):
    val extractedColumn = TestDataframes.simpleTestFrame.get(age)
    val expectedValues = DataColumn.of[Int]("Age", Seq(15, 20, 100))

    assertEquals(extractedColumn.values, expectedValues.values)

  test("A row can be accessed by index"):
    val extractedRow = TestDataframes.simpleTestFrame(1)
    val extractedValues =
      Seq(extractedRow.get(name), extractedRow.get(age))
    val expectedValues = Seq("Bob", 20)

    assertEquals(extractedValues, expectedValues)

  test("Rows can be iterated over using map"):
    val extractedRows =
      TestDataframes.simpleTestFrame.map(row => row.get(name))
    val expectedValues = Seq("Alice", "Bob", "Charlie")

    assertEquals(extractedRows, expectedValues)
