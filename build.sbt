ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val versionSpark: String = "3.5.1"

lazy val coreModule: Project = (project in file("sparkles-core"))
  .settings(
    name := "sparkles-core",
    libraryDependencies ++= Seq(
      // Provided deps:
      "org.apache.spark" %% "spark-core" % versionSpark % Provided,
      "org.apache.spark" %% "spark-sql" % versionSpark % Provided,
      "org.apache.spark" %% "spark-sql-api" % versionSpark % Provided,
      "org.apache.spark" %% "spark-graphx" % versionSpark % Provided,
      "org.apache.spark" %% "spark-catalyst" % versionSpark % Provided
    )
  )

lazy val modules: Seq[ProjectReference] = Seq(coreModule)
lazy val root: Project = (project in file("."))
  .settings(name := "sparkles")
  .aggregate(modules: _*)
