package scotlinframe

import org.jetbrains.kotlinx.dataframe.{api => ktApi}
import org.jetbrains.kotlinx.dataframe.api.{
    ConstructorsKt => KtConstructorsKt, 
    DataColumnTypeKt => KtDataColumnTypeKt,
    Infer => KtInfer, 
    ValuesKt => KtValuesKt,
    NullsKt => KtNullsKt
}
import org.jetbrains.kotlinx.dataframe.{
    DataColumn => KtDataColumn, 
    DataColumnKt => KtDataColumnKt
}


import scala.jdk.CollectionConverters.*
 import org.jetbrains.kotlinx.dataframe.DataFrameKt

class DataColumn[A](private[scotlinframe] val ktDataColumn : KtDataColumn[A]):
  override def toString(): String = ktDataColumn.toString()

  def name : String = ktDataColumn.name()

  def get(index : Int) : A = ktDataColumn.get(index)
  def values : Seq[A] = ktDataColumn.values().asScala.toSeq
  
  def get(range : Range) : DataColumn[A] = get(range.toSeq)
  
  def get(iterable : Iterable[Int]) : DataColumn[A] = 
    val it = iterable.map(_.asInstanceOf[java.lang.Integer]).asJava
    DataColumn(ktDataColumn.get(it))

  def distinct() : DataColumn[A] = DataColumn(ktDataColumn.distinct())

  def dropNA() : DataColumn[A] = DataColumn(KtNullsKt.dropNA(ktDataColumn))


object DataColumn:
    def ofInts(name : String, values : Seq[Int]) : DataColumn[Int | Null] = 
        val kclass  = kotlin.jvm.JvmClassMappingKt.getKotlinClass[Int](classOf[Int])
        val ktype = kotlin.reflect.full.KClasses.getDefaultType(kclass)
        val col = KtDataColumn.Companion.createValueColumn(name, values.asJava, ktype, KtInfer.None, null)
        DataColumn[Int | Null](col)
    
    def ofDouble(name : String, values : Seq[Double]) : DataColumn[Double | Null] = 
        val kclass  = kotlin.jvm.JvmClassMappingKt.getKotlinClass[Double](classOf[Double])
        val ktype = kotlin.reflect.full.KClasses.getDefaultType(kclass)
        val col = KtDataColumn.Companion.createValueColumn(name, values.asJava, ktype, KtInfer.None, null)
        DataColumn[Double | Null](col)

    def ofStrings(name : String, values : Seq[String]) : DataColumn[String | Null] = 
        val kclass  = kotlin.jvm.JvmClassMappingKt.getKotlinClass[String](classOf[String])
        val ktype = kotlin.reflect.full.KClasses.getDefaultType(kclass)
        val col = KtDataColumn.Companion.createValueColumn(name, values.asJava, ktype, KtInfer.None, null)
        DataColumn[String | Null](col)
