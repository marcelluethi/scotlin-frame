package scotlinframe

case class ParserOptions(
    locale: java.util.Locale,
    dateTimeFormatter: java.time.format.DateTimeFormatter,
    dateTimePattern: String,
    nullStrings: Set[String]
)
