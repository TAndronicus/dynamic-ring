name := "dynamic-ring"

version := "0.1"

scalaVersion := "2.12.11"
val sparkVersion = "3.0.0"

libraryDependencies ++= List(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
)
