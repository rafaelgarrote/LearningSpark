name := "LearningSpark"

version := "1.0"

scalaVersion := "2.10.4"

fork := true

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.1",
  "org.apache.spark" %% "spark-sql" % "1.2.1",
  "org.apache.spark" %% "spark-hive" % "1.2.1"
)