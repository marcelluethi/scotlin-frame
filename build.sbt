ThisBuild / organization := "ch.unibas.cs.gravis"
ThisBuild / version := "0.1-SNAPSHOT"
ThisBuild / scalaVersion := "3.3.0"

ThisBuild / homepage := Some(url("https://github.com/marcelluethi/scotlin-frame"))
ThisBuild / licenses += ("Apache-2.0", url(
  "http://www.apache.org/licenses/LICENSE-2.0"
))
ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/marcelluethi/scotlin-frame"),
    "git@github.com:marcelluethi/scotlin-frame.git"
  )
)
ThisBuild / developers := List(
  Developer(
    "marcelluethi",
    "marcelluethi",
    "marcel.luethi@unibas.ch",
    url("https://github.com/marcelluethi")
  )
)
ThisBuild / versionScheme := Some("early-semver")

lazy val root = (project in file("."))
  .settings(
    name := """scala-dataframe""",
    publishMavenStyle := true,
    publishTo := Some(
      if (isSnapshot.value)
        Opts.resolver.sonatypeSnapshots
      else
        Opts.resolver.sonatypeStaging
    ),
    resolvers ++= Seq(
      Resolver.jcenterRepo,
      Resolver.sonatypeRepo("releases"),
      Resolver.sonatypeRepo("snapshots")
    ),
    scalacOptions ++=  Seq(
          "-encoding",
          "UTF-8",
          "-Xlint",
          "-deprecation",
          "-unchecked",
          "-feature",
          "-target:jvm-1.8"
        ),
    javacOptions ++= (CrossVersion.partialVersion(scalaVersion.value) match {
      case _ => Seq("-source", "1.8", "-target", "1.8")
    }),
   
    libraryDependencies ++= Seq(
      "org.jetbrains.kotlinx" % "dataframe" % "0.11.0",
      "org.scalameta" %% "munit" % "0.7.29" % Test
    ),
    mdocIn := new java.io.File("docs/mdoc"),
    mdocOut := new java.io.File("docs/")
  )
  .enablePlugins(MdocPlugin)
