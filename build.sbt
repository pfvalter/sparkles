ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val sparkVersion: String = "3.5.5"
lazy val scalaTestVersion: String = "3.2.19"

lazy val coreModule: Project = (project in file("sparkles-core"))
  .settings(
    name := "sparkles-core",
    libraryDependencies ++= Seq(
      // Spark Deps. Set to provided as they are quite heavy on the jar if you don't state it:
      "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-sql-api" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-graphx" % sparkVersion % Provided,
      "org.apache.spark" %% "spark-catalyst" % sparkVersion % Provided,
      // ScalaTest Deps:
      "org.scalatest" %% "scalatest-core" % scalaTestVersion,
      "org.scalatest" %% "scalatest-flatspec" % scalaTestVersion,
      "org.scalatest" %% "scalatest-matchers-core" % scalaTestVersion,
      "org.scalatest" %% "scalatest-shouldmatchers" % scalaTestVersion,
    )
  )

lazy val modules: Seq[ProjectReference] = Seq(coreModule)
lazy val root: Project = (project in file("."))
  .settings(name := "sparkles")
  .aggregate(modules *)
