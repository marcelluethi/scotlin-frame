package scotlinframe.dataframe

import org.jetbrains.kotlinx.dataframe.io.DataFrameHtmlData
import scotlinframe.DataFrame
import java.io.File
import scala.util.Try

class HTML(ktHtml: DataFrameHtmlData):
  def writeToFile(file: File): Try[Unit] = Try:
    ktHtml.writeHTML(file)

  def openInBrowser(): Try[Unit] = Try:
    ktHtml.openInBrowser()
