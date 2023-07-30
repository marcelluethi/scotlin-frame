package scotlinframe.examples

import java.net.URL
import scala.util.matching.Regex
import scotlinframe.*

@main
def datacleaningExample(): Unit =

// create columns
  val df = DataFrame.ofColumns(
    DataColumn.of(
      "fromTo",
      Seq(
        "LoNDon_paris",
        "MAdrid_miLAN",
        "londON_StockhOlm",
        "Budapest_PaRis",
        "Brussels_londOn"
      )
    ),
    DataColumn.of(
      "flightNumber",
      Seq(10045.0, Double.NaN, 10065.0, Double.NaN, 10085.0)
    ),
    DataColumn.of(
      "airline",
      Seq(
        "KLM(!)",
        "12. Air France",
        "(British Airways. )",
        "12. Air France",
        "'Swiss Air'"
      )
    )
  )
  df.print()

  // format: off
  val clean = 
    df
    .fillNA("flightNumber").mapRow(row =>
      row.prev
        .map(prevRow => prevRow.get[Double]("flightNumber").nn + 10)
        .getOrElse(0)
    )
    .convert("flightNumber").to[Int]
    .update("airline").mapValue((value : String)=>
      "([a-zA-Z\\s]+)".r.findFirstIn(value).getOrElse("")
    )
    .split("fromTo").by(
      (value : String) =>
        val parts = value.nn.split("_")
        (parts(0), parts(1))
      ).into("origin", "destination")
    .update("origin").mapValue((origin : String) => origin.nn.toLowerCase)
    .update("destination").mapValue((dest : String) => dest.nn.toLowerCase)
    .print()
// format: on
