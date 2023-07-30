package scotlinframe

import java.net.URL

@main
def test(): Unit = 
//    val df = DataFrame.read(java.io.File("test.csv"), Seq("Name", "age")).get
    // println(df)
    // // val col = df.get[String]("Name")
    // println("Name only: \n: " +col)
    // println("Age only: \n: " + df.get[Int]("age"))
    // println("Name and age: \n: " + df.get(Seq("Name", "age")))
    
    // val df = DataFrame.ofColumns(Seq(
    //     DataColumn.ofStrings("Name", Seq("John", "Jane", "Koe")),
    //     DataColumn.ofInts("age", Seq(20, 30, 40))
    // ))

    // val df2 = DataFrame.ofColumns(Seq(
    //     DataColumn.ofStrings("Lastname", Seq("Smith", "Jones", "Miller")),
    //     DataColumn.ofDouble("Birthday", Seq(1.0, 2.0, 3.0))
    // ))

    // val df3 = df.add(df2)

    // df3.print()
    // // df.dropNA(whereAllNA = true).print()
    val df = DataFrame.readCSVFromUrl(URL("https://raw.githubusercontent.com/Kotlin/dataframe/master/data/jetbrains_repositories.csv")).get
    df.describe().print()