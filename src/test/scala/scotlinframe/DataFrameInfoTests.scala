package scotlinframe

import munit.*

class DataFrameInfoTests extends FunSuite:

  test("yields correct number of rows"):
    assertEquals(TestDataframes.simpleTestFrameWithDuplicateRow.count, 4)

  test("yields correct number of disctinct rows"):
    assertEquals(
      TestDataframes.simpleTestFrameWithDuplicateRow.countDistinct,
      3
    )

  test("yields correct number of disctinct rows when column is specified"):
    assertEquals(
      TestDataframes.simpleTestFrameWithDuplicateRow.countDistinct("Age"),
      2
    )

  test("yields the correct number of distinct columns"):
    assertEquals(
      TestDataframes.simpleTestFrameWithDuplicateRow.columnsCount,
      2
    )

  test("yields the correct names of the columns"):
    assertEquals(
      TestDataframes.simpleTestFrame.columnNames,
      Seq("Name", "Age")
    )

  test("reports the correct types"):
    val expectedTypes = Seq(
      DataType.String,
      DataType.Int,
      DataType.Long,
      DataType.Float,
      DataType.Double,
      DataType.Boolean,
      DataType.LocalTime,
      DataType.LocalDate,
      DataType.LocalDateTime
    )
    assertEquals(
      TestDataframes.testFrameWithEveryType.columnTypes,
      expectedTypes
    )

  test("head yields the correct number of rows"):
    assertEquals(TestDataframes.simpleTestFrame.head(2).count, 2)
