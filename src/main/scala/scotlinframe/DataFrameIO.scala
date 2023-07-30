package scotlinframe


import org.jetbrains.kotlinx.dataframe.io.{
    GuessKt => KtGuessKt,
    CSV => KtCSV,
    CsvKt => KtCsvKt,
    JsonKt => KtJsonKt,
    JSON => KtJson
}


import org.jetbrains.kotlinx.dataframe.{
    DataColumn => KtDataColumn,
    DataColumnKt => KtDataColumnKt,
    DataRow => KtDataRow,  
    DataRowKt => KtDataRowKt,
    DataFrame => KtDataFrame, 
    DataFrameKt => KtDataFrameKt,
}

import scala.jdk.CollectionConverters.*
import java.io.File
import scala.util.Try
import java.net.URL

object DataFrameIO:

    def readCSVFromFile(file : File, delimiter : Char = ',') : Try[DataFrame]  = Try:
        val frame = KtCsvKt.readCSV(
            KtDataFrame.Companion, 
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

    def readCSVFromUrl(url : URL, delimiter : Char = ',') : Try[DataFrame]  = Try:
        val frame = KtCsvKt.readCSV(
            KtDataFrame.Companion, 
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

    def readJsonFromUrl(url : URL) : Try[DataFrame] = Try:
        val frame = KtJsonKt.readJson(
            KtDataFrame.Companion, 
            url,
            List.empty[String].asJava, // headers
            List.empty.asJava, // keyValuePaths
            KtJson.TypeClashTactic.ARRAY_AND_VALUE_COLUMNS
            )
        DataFrame(frame)

    def readJsonFromFile(file : File) : Try[DataFrame] = Try:
        val frame = KtJsonKt.readJson(
            KtDataFrame.Companion, 
            file,
            List.empty[String].asJava, // headers
            List.empty.asJava, // keyValuePaths
            KtJson.TypeClashTactic.ARRAY_AND_VALUE_COLUMNS
            )
        DataFrame(frame)
