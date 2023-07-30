package scotlinframe

import munit.*
import java.io.File
import scala.util.Failure
import scala.util.Success

import scotlinframe.DateTime.LocalDate
import scotlinframe.dataframe.DataFrameIO

class DataFrameIOTests extends FunSuite:

  test("can create a dataframe from a csv file"):
    val file = File(getClass.getResource("/persons.csv").getPath)

    DataFrame.readCSVFromFile(file) match
      case Failure(error) => fail(s"Failed to read csv file: $error")
      case Success(df) =>
        val x = df.get[LocalDate]("Birthdate").get(0)
        assertEquals(df.get[String]("Name").values, Seq("Joe", "Bob"))
        assertEquals(df.get[Int]("Age").values, Seq(30, 40))
        assertEquals(
          df.get[LocalDate]("Birthdate").values,
          Seq(LocalDate.of(1980, 9, 8), LocalDate.of(1970, 2, 12))
        )

  test("can write a dataframe in csv format"):
    val originalDf = TestDataframes.simpleTestFrame

    // write to temp file
    val tempFile = File.createTempFile("test", ".csv")
    tempFile.deleteOnExit()

    DataFrameIO.writeCSV(originalDf, tempFile) match
      case Failure(error) => fail(s"Failed to write csv file: $error")
      case Success(_) =>
        val rereadDf = DataFrameIO.readCSVFromFile(tempFile).get
        // comparing every cell does not really make sense. So we assert equality over describes
        assertNoDiff(
          originalDf.describe.toString(),
          rereadDf.describe.toString()
        )
