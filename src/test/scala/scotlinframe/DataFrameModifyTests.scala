package scotlinframe

import munit.*

class DataFrameModifyTests extends FunSuite:

  val age = "Age".asColumn[Int]
  val name = "Name".asColumn[String]

  test(
    "A dataframe consisting of the specified rows can be accessed by specifying a range"
  ):
    val df = TestDataframes.simpleTestFrame

    val dfSliced = df(1 until df.rowsCount)
    val dfDropped = df.drop(1)

    assertEquals(dfSliced.describe.toString(), dfDropped.describe.toString())

  test(
    "A dataframe consisting of the specified rows can be accessed by specifying an iterable"
  ):
    val df = TestDataframes.simpleTestFrame

    val dfSliced = df(Seq(1, 2))
    val dfDropped = df.drop(1)

    assertEquals(dfSliced.describe.toString(), dfDropped.describe.toString())

  test(
    "A dataframe consisting of the specified rows can be accessed by filtering"
  ):
    val df = TestDataframes.simpleTestFrame
    val filteredDf = df.filter(r => r.get(age).nn > 17)
    assert(filteredDf.get(age).values.forall(v => v.nn > 17))

  test("A dataframe can be sorted by a column"):
    val df = DataFrame.ofColumns(
      DataColumn.of[String]("Name", Seq("Alice", "Bob", "Charlie")),
      DataColumn.of[Int]("Age", Seq(10, 20, 30))
    )

    val sortedDf = df.sortBy("Age")
    assert(sortedDf.get(age).values.map(_.nn) == Seq(10, 20, 30))

  test("Values in a dataframe can be updated with a function using a rowMap"):
    val df = TestDataframes.simpleTestFrame
    val updatedDf =
      df.update(age).mapRow(row => { row.get(age).nn + 1 })
    assertEquals(
      updatedDf.get(age).values.map(_.nn),
      df.get(age).values.map(_.nn + 1)
    )

  test("Values in a dataframe can be updated with a function using a valueMap"):
    val df = TestDataframes.simpleTestFrame
    val updatedDf =
      df.update(age).mapValue[Int, Int](age => age.nn + 1)
    assertEquals(
      updatedDf.get(age).values.map(_.nn),
      df.get(age).values.map(_.nn + 1)
    )

  test("A column can be inserted into a dataframe"):
    val df = TestDataframes.simpleTestFrame
    val updatedDf =
      df.insert(DataColumn.of[String]("Test", Seq("a", "b", "c"))).after("Name".asColumn)
    assertEquals(updatedDf.get("Test".asColumn[String]).values, Seq("a", "b", "c"))
    assertEquals(updatedDf.columnNames, Seq("Name", "Test", "Age"))

  test("A column can be replace by another column"):
    val df = TestDataframes.simpleTestFrame
    val updatedDf = df
      .replace("Name".asColumn)
      .withColumn(DataColumn.of[String]("Test", Seq("a", "b", "c")))
    assertEquals(updatedDf.get("Test".asColumn[String]).values, Seq("a", "b", "c"))
    assertEquals(updatedDf.columnNames, Seq("Test", "Age"))

  test("A column can be removed from a dataframe"):
    val df = TestDataframes.simpleTestFrame
    val updatedDf = df.remove("Name".asColumn)
    assertEquals(updatedDf.columnNames, Seq("Age"))

  test("A dataframe can be extended by mapping over the rows"):
    val df = TestDataframes.simpleTestFrame
    val extendedDf = df
      .mapRows(row => row.get(age).nn + 1, "AgePlus1".asColumn[Int])
      .insertAfter("Age".asColumn)
    assertEquals(
      extendedDf.get[Int]("AgePlus1".asColumn).values,
      df.get(age).values.map(_.nn + 1)
    )

  test("A data can be extended by several columns by mapping over the rows"):
    val df = TestDataframes.simpleTestFrame
    val extendedDf = df
      .mapRows(row =>
        Map(
          "NameUppercase" -> row.get(name).nn.toUpperCase,
          "NameLowercase" -> row.get(name).nn.toLowerCase
        )
      )
      .insertAfter(name)

    assertEquals(
      extendedDf.columnNames,
      Seq("Name", "NameUppercase", "NameLowercase", "Age")
    )
