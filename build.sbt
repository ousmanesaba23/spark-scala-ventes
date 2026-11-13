ThisBuild / organization := "fr.master.spark"
ThisBuild / version := "1.0.0"
ThisBuild / scalaVersion := "2.13.18"

lazy val root = (project in file("."))
  .settings(
    name := "spark-scala-ventes",
    Compile / mainClass := Some("fr.master.sparkventes.SalesAnalytics"),
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "4.3.0" % Provided,
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-encoding", "UTF-8")
  )
