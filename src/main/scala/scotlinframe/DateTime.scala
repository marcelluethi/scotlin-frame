package scotlinframe

object DateTime:

  type LocalDateTime = kotlinx.datetime.LocalDateTime
  object LocalDateTime:
    def of(
        year: Int,
        month: Int,
        dayOfMonth: Int,
        hour: Int,
        minute: Int,
        second: Int,
        nanosecond: Int
    ): LocalDateTime =
      kotlinx.datetime.LocalDateTime(
        year,
        month,
        dayOfMonth,
        hour,
        minute,
        second,
        nanosecond
      )

  type LocalDate = kotlinx.datetime.LocalDate
  object LocalDate:
    def of(year: Int, month: Int, dayOfMonth: Int): LocalDate =
      kotlinx.datetime.LocalDate(year, month, dayOfMonth)

  type LocalTime = kotlinx.datetime.LocalTime
  object LocalTime:
    def of(hour: Int, minute: Int, second: Int, nanosecond: Int): LocalTime =
      kotlinx.datetime.LocalTime(hour, minute, second, nanosecond)

  type Duration = kotlin.time.Duration
  object Duration:
    def days(days: Int): Duration =
      kotlin.time.Duration.Companion
        .`parseOrNull-FghU774`(java.time.Duration.ofDays(days).toString())

    def hours(hours: Int): Duration =
      kotlin.time.Duration.Companion
        .`parseOrNull-FghU774`(java.time.Duration.ofHours(hours).toString())

    def minutes(minutes: Int): Duration =
      kotlin.time.Duration.Companion
        .`parseOrNull-FghU774`(java.time.Duration.ofMinutes(minutes).toString())

    def seconds(seconds: Int): Duration =
      kotlin.time.Duration.Companion
        .`parseOrNull-FghU774`(java.time.Duration.ofSeconds(seconds).toString())
