package scotlinframe.dataframe

import scotlinframe.utils.ToKType
import scotlinframe.DataFrame

import org.jetbrains.kotlinx.dataframe.{io => ktio}
import org.jetbrains.kotlinx.{dataframe => ktdataframe}

import scala.jdk.CollectionConverters.*
import java.io.File
import scala.util.Try
import java.net.URL
import org.apache.commons.csv.CSVFormat

object DataFrameIO:

  def readCSVFromFile(file: File, delimiter: Char = ','): Try[DataFrame] = Try:
    val frame = ktio.CsvKt.readCSV(
      ktdataframe.DataFrame.Companion,
      file,
      delimiter,
      List.empty[String].asJava,
      Map.empty.asJava, // coltypes
      0, // skiplines
      null, // readlines
      true,
      java.nio.charset.Charset.forName("UTF-8"),
      null
    )
    DataFrame(frame)

  def readCSVFromUrl(url: URL, delimiter: Char = ','): Try[DataFrame] = Try:
    val frame = ktio.CsvKt.readCSV(
      ktdataframe.DataFrame.Companion,
      url,
      delimiter,
      List.empty[String].asJava,
      Map.empty.asJava,
      0,
      null,
      true,
      java.nio.charset.Charset.forName("UTF-8"),
      null
    )
    DataFrame(frame)

  def readJsonFromUrl(url: URL): Try[DataFrame] = Try:
    val frame = ktio.JsonKt.readJson(
      ktdataframe.DataFrame.Companion,
      url,
      List.empty[String].asJava, // headers
      List.empty.asJava, // keyValuePaths
      ktio.JSON.TypeClashTactic.ARRAY_AND_VALUE_COLUMNS
    )
    DataFrame(frame)

  def readJsonFromFile(file: File): Try[DataFrame] = Try:
    val frame = ktio.JsonKt.readJson(
      ktdataframe.DataFrame.Companion,
      file,
      List.empty[String].asJava, // headers
      List.empty.asJava, // keyValuePaths
      ktio.JSON.TypeClashTactic.ARRAY_AND_VALUE_COLUMNS
    )
    DataFrame(frame)

  def writeCSV(
      dataframe: DataFrame,
      file: File,
      format: CSVFormat = CSVFormat.DEFAULT
  ): Try[Unit] = Try:
    ktio.CsvKt.writeCSV(dataframe.ktDataFrame, file, format)

  def writeJson(
      dataframe: DataFrame,
      file: File,
      prettyPrint: Boolean = false,
      cannonical: Boolean = false
  ): Try[Unit] = Try:
    ktio.JsonKt.writeJson(dataframe.ktDataFrame, file, prettyPrint, cannonical)
