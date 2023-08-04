package scotlinframe.utils

import kotlin.reflect.KType
import scotlinframe.DateTime.*
import scotlinframe.DataFrame
import org.jetbrains.kotlinx.{dataframe => ktdataframe}

object KotlinToScotlinFrameType:
  def fromKotlin[A](kotlinobj: Any): A =
    kotlinobj match
      // kotlin datafames are cast into dataframes and wrapped into a scotlinframe dataframes
      // as casting happens only when we work with an object, this alleviates the need of explicit
      // representation of dataframe columns
      case ktDataFrame: ktdataframe.DataFrame[?] => DataFrame(ktDataFrame).asInstanceOf[A]
      case kotlinobj                             => kotlinobj.asInstanceOf[A]

trait ToKType[A]:
  def ktype: KType

object ToKType:

  given ToKType[Int] with
    def ktype: KType =
      val kclass =
        kotlin.jvm.JvmClassMappingKt.getKotlinClass[Int](classOf[Int])
      kotlin.reflect.full.KClasses.getDefaultType(kclass)

  given ToKType[Short] with
    def ktype: KType =
      val kclass =
        kotlin.jvm.JvmClassMappingKt.getKotlinClass[Short](classOf[Short])
      kotlin.reflect.full.KClasses.getDefaultType(kclass)

  given ToKType[Double] with
    def ktype: KType =
      val kclass =
        kotlin.jvm.JvmClassMappingKt.getKotlinClass[Double](classOf[Double])
      kotlin.reflect.full.KClasses.getDefaultType(kclass)

  given ToKType[Float] with
    def ktype: KType =
      val kclass =
        kotlin.jvm.JvmClassMappingKt.getKotlinClass[Float](classOf[Float])
      kotlin.reflect.full.KClasses.getDefaultType(kclass)

  given ToKType[Boolean] with
    def ktype: KType =
      val kclass =
        kotlin.jvm.JvmClassMappingKt.getKotlinClass[Boolean](classOf[Boolean])
      kotlin.reflect.full.KClasses.getDefaultType(kclass)

  given ToKType[Long] with
    def ktype: KType =
      val kclass =
        kotlin.jvm.JvmClassMappingKt.getKotlinClass[Long](classOf[Long])
      kotlin.reflect.full.KClasses.getDefaultType(kclass)

  given ToKType[String] with
    def ktype: KType =
      val kclass =
        kotlin.jvm.JvmClassMappingKt.getKotlinClass[String](classOf[String])
      kotlin.reflect.full.KClasses.getDefaultType(kclass)

  given ToKType[LocalDateTime] with
    def ktype: KType =
      val kclass =
        kotlin.jvm.JvmClassMappingKt
          .getKotlinClass[LocalDateTime](classOf[LocalDateTime])
      kotlin.reflect.full.KClasses.getDefaultType(kclass)

  given ToKType[LocalTime] with
    def ktype: KType =
      val kclass =
        kotlin.jvm.JvmClassMappingKt
          .getKotlinClass[LocalTime](classOf[LocalTime])
      kotlin.reflect.full.KClasses.getDefaultType(kclass)

  given ToKType[LocalDate] with
    def ktype: KType =
      val kclass =
        kotlin.jvm.JvmClassMappingKt
          .getKotlinClass[LocalDate](classOf[LocalDate])
      kotlin.reflect.full.KClasses.getDefaultType(kclass)

  given ToKType[Duration] with
    def ktype: KType =
      val kclass =
        kotlin.jvm.JvmClassMappingKt.getKotlinClass[Duration](classOf[Duration])
      kotlin.reflect.full.KClasses.getDefaultType(kclass)

  given [A](using toKType: ToKType[A]): ToKType[A | Null] with
    def ktype: KType = toKType.ktype

object FromKType:
  def ktypeToSimpleString(ktype: KType): String =
    ktype.toString().split(".").last
