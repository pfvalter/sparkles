ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val coreModule: Project = Project("sparkles-core", new File("sparkles-core"))
  .settings(name := "sparkles-core")

lazy val modules: Seq[ProjectReference] = Seq(coreModule)
lazy val root: Project = Project("sparkles", new File("."))
  .settings(name := "sparkles")
  .aggregate(modules: _*)
